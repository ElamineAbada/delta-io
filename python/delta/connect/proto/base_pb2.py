#
# Copyright (2024) The Delta Lake Project Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: delta/connect/base.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database

# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(
    b'\n\x18\x64\x65lta/connect/base.proto\x12\rdelta.connect"\xad\x02\n\nDeltaTable\x12\x34\n\x04path\x18\x01 \x01(\x0b\x32\x1e.delta.connect.DeltaTable.PathH\x00R\x04path\x12-\n\x12table_or_view_name\x18\x02 \x01(\tH\x00R\x0ftableOrViewName\x1a\xaa\x01\n\x04Path\x12\x12\n\x04path\x18\x01 \x01(\tR\x04path\x12O\n\x0bhadoop_conf\x18\x02 \x03(\x0b\x32..delta.connect.DeltaTable.Path.HadoopConfEntryR\nhadoopConf\x1a=\n\x0fHadoopConfEntry\x12\x10\n\x03key\x18\x01 \x01(\tR\x03key\x12\x14\n\x05value\x18\x02 \x01(\tR\x05value:\x02\x38\x01\x42\r\n\x0b\x61\x63\x63\x65ss_typeB\x1a\n\x16io.delta.connect.protoP\x01\x62\x06proto3'
)

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, "delta.connect.proto.base_pb2", globals())
if _descriptor._USE_C_DESCRIPTORS == False:
    DESCRIPTOR._options = None
    DESCRIPTOR._serialized_options = b"\n\026io.delta.connect.protoP\001"
    _DELTATABLE_PATH_HADOOPCONFENTRY._options = None
    _DELTATABLE_PATH_HADOOPCONFENTRY._serialized_options = b"8\001"
    _DELTATABLE._serialized_start = 44
    _DELTATABLE._serialized_end = 345
    _DELTATABLE_PATH._serialized_start = 160
    _DELTATABLE_PATH._serialized_end = 330
    _DELTATABLE_PATH_HADOOPCONFENTRY._serialized_start = 269
    _DELTATABLE_PATH_HADOOPCONFENTRY._serialized_end = 330
# @@protoc_insertion_point(module_scope)
