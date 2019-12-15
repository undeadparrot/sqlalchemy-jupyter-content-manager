import copy
import logging
import pathlib

from notebook.services.contents.manager import ContentsManager
from notebook.services.contents.checkpoints import Checkpoints, GenericCheckpointsMixin
from notebook.services.contents.tests.test_manager import TestContentsManager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import borestore
from borestore import model

logging.basicConfig()


def new_directory(path):
    return dict(
        type="directory",
        format="json",
        mimetype=None,
        name=path,
        path=path,
        writable=True,
        #
        created=1,
        last_modified=1,
        #
        content=[],
    )


def new_file(type_, path):
    return dict(
        type=type_,
        format="json",
        mimetype=None,
        name=path,
        path=path,
        writable=True,
        created=1,
        last_modified=1,
    )


class BoreStoreCheckpointsManager(GenericCheckpointsMixin, Checkpoints):
    """requires the following methods:"""

    def create_file_checkpoint(self, content, format, path):
        """ -> checkpoint model"""
        return dict(id="", type="file", content="", format=None)

    def create_notebook_checkpoint(self, nb, path):
        """ -> checkpoint model"""
        return dict(id="", type="notebook", content="")

    def get_file_checkpoint(self, checkpoint_id, path):
        """ -> {'type': 'file', 'content': <str>, 'format': {'text', 'base64'}}"""
        return dict(id=checkpoint_id, type="file", content="", format=None)

    def get_notebook_checkpoint(self, checkpoint_id, path):
        """ -> {'type': 'notebook', 'content': <output of nbformat.read>}"""
        return dict(id=checkpoint_id, type="notebook", content="")

    def delete_checkpoint(self, checkpoint_id, path):
        """deletes a checkpoint for a file"""

    def list_checkpoints(self, path):
        """returns a list of checkpoint models for a given file,
        default just does one per file
        """
        return []

    def rename_checkpoint(self, checkpoint_id, old_path, new_path):
        """renames checkpoint from old path to new path"""


class BoreStoreContentsManager(ContentsManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.engine = create_engine("postgresql://jupyter@localhost/jupyter", echo=True)
        self.mksession = sessionmaker(self.engine)

        self.files = {"": new_directory("")}
        self.checkpoints = BoreStoreCheckpointsManager()

    def _with_session(f):
        def inner(self, *args, **kwargs):
            self.session = self.mksession()
            try:
                result = f(self, *args, **kwargs)
                return result
            except Exception:
                self.session.rollback()
                raise
            finally:
                self.session.close()
        return inner

    @_with_session
    def get(self, path, content=True, **kwargs):
        type_ = kwargs.get("type")
        format_ = kwargs.get("format")
        logging.warning(
            "BoreStore get: `%s` `%s` `%s` `%s`", path, content, type_, format_
        )

        node = self.session.query(model.Node).filter_by(stored_path=path.lstrip('/')).first()

        return node.serialize(content=content)

    @_with_session
    def save(self, model, path):
        type_ = model.get("type")
        format_ = model.get("format")
        logging.warning("BoreStore save: `%s` `%s`", path, type_)

        node = self.session.query(borestore.model.Node).filter_by(stored_path=path.lstrip('/')).first()
        if node:
            node.data = model['content']

        else:
            route = pathlib.Path(path)

            parent = (
                self.session.query(borestore.model.Directory).filter_by(filename=str(route.parent)).first()
            )

            if type_ == "notebook":
                node = borestore.model.Notebook(
                    parent=parent, filename=route.name, data=model["content"]
                )
            elif type_ == "directory":
                node = borestore.model.Directory(parent=parent, filename=route.name)
            elif type_ == "file":
                node = borestore.model.File(parent=parent, filename=route.name, data=model["content"])
            else:
                raise Exception(f"Unknown type {path} {model}")

            self.session.add(node)

        self.session.commit()
        return node.serialize(content=False)

    @_with_session
    def rename_file(self, old_path, path):
        logging.warning("BoreStore rename_file: `%s` `%s`", old_path, path)

    @_with_session
    def delete_file(self, path):
        logging.warning("BoreStore delete_file: `%s`", path)
        node = self.session.query(model.Node).filter_by(stored_path=path.lstrip('/')).one()
        self.session.remove(node)
        self.session.commit()

    @_with_session
    def file_exists(self, path):
        logging.warning("BoreStore file_exists: `%s`", path)

        node = self.session.query(model.Node).filter_by(stored_path=path.lstrip('/')).first()

        return node and not isinstance(node, model.Directory)

    @_with_session
    def dir_exists(self, path):
        logging.warning("BoreStore dir_exists: `%s`", path)

        node = self.session.query(model.Directory).filter_by(stored_path=path.lstrip('/')).first()

        return node != None

    @_with_session
    def is_hidden(self, path):
        logging.warning("BoreStore is_hidden: %s", path)
        return False


class DictContentsManager(ContentsManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.engine = create_engine("postgresql://jupyter@localhost/jupyter", echo=True)
        self.mksession = sessionmaker(self.engine)

        self.files = {"": new_directory("")}
        self.checkpoints = BoreStoreCheckpointsManager()

    def get(self, path, content=True, **kwargs):
        type_ = kwargs.get("type")
        format_ = kwargs.get("format")
        logging.warning(
            "BoreStore get: `%s` `%s` `%s` `%s`", path, content, type_, format_
        )

        model = self.files[path.lstrip("/")]

        response = copy.deepcopy(model)
        if not content:
            response["content"] = None
            response["format"] = None

        return response

    def save(self, model, path):
        type_ = model.get("type")
        format_ = model.get("format")
        model_ = new_file(type_, path)
        model_.update(model)
        if type_ == "notebook":
            self.files[path] = model_
        elif type_ == "directory":
            self.files[path] = model_
        elif type_ == "file":
            if format_ == "text":
                self.files[path] = model_
            elif format_ == "base64":
                self.files[path] = model_
        else:
            raise Exception(f"Unknown type {path} {model}")
        logging.warning("BoreStore save: `%s` `%s`", path, model)
        response = copy.deepcopy(model_)
        response["content"] = None
        response["format"] = None
        self.files[""]["content"].append(response)
        return response

    def rename_file(self, old_path, path):
        logging.warning("BoreStore rename_file: `%s` `%s`", old_path, path)

    def delete_file(self, path):
        logging.warning("BoreStore delete_file: `%s`", path)

    def file_exists(self, path):
        logging.warning("BoreStore file_exists: `%s`", path)
        return self.files.get(path, {}).get("type") in {"file", "notebook"}

    def dir_exists(self, path):
        logging.warning("BoreStore dir_exists: `%s`", path)
        return self.files.get(path, {}).get("type") in {"directory"}

    def is_hidden(self, path):
        logging.warning("BoreStore is_hidden: %s", path)
        return False
