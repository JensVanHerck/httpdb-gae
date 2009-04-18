#!/usr/bin/env python
#
# Copyright 2009 Wouter Simons
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from google.appengine.ext import webapp
from google.appengine.ext import db
from google.appengine.ext.webapp.util import run_wsgi_app

class Folder(db.Model):
  uuidOwner = db.StringProperty(multiline=False)
  folderName = db.StringProperty(multiline=False)

class StoredData(db.Model):
  uuidOwner = db.StringProperty(multiline=False)
  name = db.StringProperty(multiline=False)
  content = db.StringProperty(multiline=True)
  folder = db.ReferenceProperty(Folder)

class HTTPDB(webapp.RequestHandler):
  def _isSl(self):
    if self.request.headers.has_key("X-Secondlife-Shard"):
      if self.request.headers["X-Secondlife-Shard"] == "Production":
        return True
    return False

  def _fileExists(self, name, uuidOwner, folder):
    _files = StoredData.gql("WHERE name = :1 AND uuidOwner = :2 AND folder = :3", name, uuidOwner, folder).fetch(1)
    if len(_files) == 1:
      self._file = _files[0]
      return True
    else:
      self._file = StoredData()
    return False

  def _folderExists(self, name, uuidOwner):
    _folders = Folder.gql("WHERE folderName = :1 AND uuidOwner = :2", name, uuidOwner).fetch(1)
    if len(_folders) == 1:
      self._folder = _folders[0]
      return True
    else:
      self._folder = Folder()
    return False

  def _dbget(self, path, mode, uuidOwner):
    if path.startswith("/"): # Ignore a leading /
      path = path[1:]
    parts = path.split("/")
    length = len(parts)
    if 1 <= length <= 2: # Either 1 or 2 items in the split string
      if mode == "list":
        if self._folderExists( parts[0], uuidOwner ):
          files = StoredData.gql("WHERE uuidOwner = :1 AND folder = :2", uuidOwner, self._folder)
          data = ""
          for item in files:
            data += item.name + "\n"
          return data.strip()
        else: return "404: Folder not found"
      else:
        if length == 1: # No parent folder
          data = StoredData.gql("WHERE uuidOwner = :1 AND name = :2 AND folder = :3", uuidOwner, parts[0], None).fetch(1)
          if len(data) == 1:
            return data[0].content
        if length == 2: # With a parent folder
          folder = Folder.gql("WHERE uuidOwner = :1 AND folderName = :2", uuidOwner, parts[0]).fetch(1)
          if len(folder) != 1:
              return "404: Folder not found"
          data = StoredData.gql("WHERE uuidOwner = :1 AND name = :2 AND folder = :3", uuidOwner, parts[1], folder[0].key()).fetch(1)
          if len(data) == 1:
            return data[0].content
    else: return "400: Only one parent folder allowed."
    return "404: Not found"

  def _dbput(self, path, value, uuidOwner):
    if path.startswith("/"): # Ignore a leading /
      path = path[1:]
    parts = path.split("/")
    length = len(parts)
    if 1 <= length <= 2: # Either 1 or 2 items in the split string
      if length == 1: # No parent folder
        if self._fileExists(parts[0], uuidOwner, None):
          # Get file and replace contents, found file has been stored in self._file
          self._file.content = value
        else:
          # File not found, new StoredData item in self._file
          self._file.uuidOwner = uuidOwner
          self._file.name = parts[0]
          self._file.content = value
        self._file.put()
        return "201: Successfully created " + path
      if length == 2: # With a parent folder
        if not self._folderExists(parts[0], uuidOwner): # A new or existing folder is always placed in self._folder
          self._folder.uuidOwner = uuidOwner
          self._folder.folderName = parts[0]
          self._folder.put()
        if self._fileExists(parts[1], uuidOwner, self._folder):
          # Get file and replace contents, found file has been stored in self._file
          self._file.content = value
        else:
          # File not found, new StoredData item in self._file
          self._file.uuidOwner = uuidOwner
          self._file.name = parts[1]
          self._file.content = value
          self._file.folder = self._folder
        self._file.put()
        return "201: Successfully created " + str(parts)
    else: return "400: Only one parent folder allowed."
    return "500: You should never encounter this"

  def _dbdel(self, path, uuidOwner):
    if path.startswith("/"): # Ignore a leading /
      path = path[1:]
    parts = path.split("/")
    length = len(parts)
    if 1 <= length <= 2: # Either 1 or 2 items in the split string
      if length == 1: # No parent folder
        if self._fileExists(parts[0], uuidOwner, None):
          self._file.delete()
        else:
          return "404: Path not found"
      if length == 2: # With a parent folder
        if not self._folderExists(parts[0], uuidOwner): # A new or existing folder is always placed in self._folder
          return "404: Path not found"
        if self._fileExists(parts[1], uuidOwner, self._folder):
          self._file.delete()
        else:
          return "404: Path not found"
    else: return "400: Only one parent folder allowed."
    return "Deleted " + path

  def get(self):
    path = self.request.path
    mode = self.request.get("mode")
    body = ""
    if mode == "":
      mode = self.request.get("m")
    if self._isSl():
      uuidOwner = self.request.headers["X-Secondlife-Owner-Key"]
      body = self._dbget(path, mode, uuidOwner)
      self.response.out.write(body)
    else:
      self.response.out.write("Path: " + path + "\n")
      self.response.out.write("Mode: " + mode + "\n")
      # self.response.out.write(str(self.request.headers))
    self.response.headers['Content-Type'] = 'text/plain'
    if body.startswith("201"):
      self.response.set_status(201)
    if body.startswith("404"):
      self.response.set_status(404)
    if body.startswith("400"):
      self.response.set_status(400)
    if body.startswith("500"):
      self.response.set_status(500)
    

  def post(self):
    self.reponse.out.write("POST not implemented")
    self.response.set_status(501)

  def put(self):
    body = ""
    path = self.request.path
    _body = self.request.body
    if self._isSl():
      uuidOwner = self.request.headers["X-Secondlife-Owner-Key"]
      body = self._dbput(path, _body, uuidOwner)
      self.response.out.write(body)
    else:
      self.response.out.write("You may only put values through SL")
      self.response.out.write(str(self.request.headers))
    if body.startswith("201"):
      self.response.set_status(201)
    if body.startswith("404"):
      self.response.set_status(404)
    if body.startswith("400"):
      self.response.set_status(400)
    if body.startswith("500"):
      self.response.set_status(500)

  def delete(self):
    body = ""
    path = self.request.path
    if self._isSl():
      uuidOwner = self.request.headers["X-Secondlife-Owner-Key"]
      _body = self._dbdel(path, uuidOwner)
      self.response.out.write(_body)
    else:
      self.response.out.write("You may only delete values through SL")
    if body.startswith("201"):
      self.response.set_status(201)
    if body.startswith("404"):
      self.response.set_status(404)
    if body.startswith("400"):
      self.response.set_status(400)
    if body.startswith("500"):
      self.response.set_status(500)

def main():
  application = webapp.WSGIApplication([('.*', HTTPDB)],
                                       debug=True)
  run_wsgi_app(application)


if __name__ == '__main__':
  main()
