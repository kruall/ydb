#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio
import time

import grpc
from grpc import aio

from ydb.public.api.grpc import ydb_keyvalue_v1_pb2_grpc as grpc_server
from ydb.public.api.grpc import ydb_keyvalue_v2_pb2_grpc as grpc_server_v2
from ydb.public.api.protos import ydb_keyvalue_pb2 as keyvalue_api


def to_bytes(value):
    if value is None:
        return None
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode('utf-8')
    raise ValueError(f"Unsupported value type for write payload: {type(value)}")


class KeyValueClient:
    def __init__(self, server, port, retry_count=1, sleep_retry_seconds=10):
        self.server = server
        self.port = port
        self.__retry_count = retry_count
        self.__retry_sleep_seconds = sleep_retry_seconds
        self._options = [
            ('grpc.max_receive_message_length', 64 * 10 ** 6),
            ('grpc.max_send_message_length', 64 * 10 ** 6),
        ]
        self._channel = grpc.insecure_channel(f"{self.server}:{self.port}", options=self._options)
        self._stub = grpc_server.KeyValueServiceStub(self._channel)
        self._stub_v2 = grpc_server_v2.KeyValueServiceStub(self._channel)
        self._async_channel = None
        self._async_stub = None
        self._async_stub_v2 = None

    def _get_invoke_callee(self, method, version):
        if version == 'v2':
            return getattr(self._stub_v2, method)
        return getattr(self._stub, method)

    def _get_async_invoke_callee(self, method, version):
        if version == 'v2':
            return getattr(self._async_stub_v2, method)
        return getattr(self._async_stub, method)

    def invoke(self, request, method, version):
        retry = self.__retry_count
        while True:
            try:
                callee = self._get_invoke_callee(method, version)
                return callee(request)
            except (RuntimeError, grpc.RpcError):
                retry -= 1
                if not retry:
                    raise
                time.sleep(self.__retry_sleep_seconds)

    async def ainvoke(self, request, method, version):
        if self._async_channel is None:
            self._async_channel = aio.insecure_channel(f"{self.server}:{self.port}", options=self._options)
            self._async_stub = grpc_server.KeyValueServiceStub(self._async_channel)
            self._async_stub_v2 = grpc_server_v2.KeyValueServiceStub(self._async_channel)

        retry = self.__retry_count
        while True:
            try:
                callee = self._get_async_invoke_callee(method, version)
                return await callee(request)
            except (RuntimeError, grpc.RpcError):
                retry -= 1
                if not retry:
                    raise
                await asyncio.sleep(self.__retry_sleep_seconds)

    def make_write_request(self, path, partition_id, kv_pairs, channel=None):
        request = keyvalue_api.ExecuteTransactionRequest()
        request.path = path
        request.partition_id = partition_id
        for key, value in kv_pairs:
            write = request.commands.add().write
            write.key = key
            write.value = to_bytes(value)
            if channel is not None:
                write.storage_channel = channel
        return request

    async def a_kv_writes(self, path, partition_id, kv_pairs, channel=None, version='v1'):
        request = self.make_write_request(path, partition_id, kv_pairs, channel)
        return await self.ainvoke(request, 'ExecuteTransaction', version)

    def make_delete_range_request(
        self, path, partition_id, from_key=None, to_key=None, from_inclusive=True, to_inclusive=False
    ):
        request = keyvalue_api.ExecuteTransactionRequest()
        request.path = path
        request.partition_id = partition_id
        delete_range = request.commands.add().delete_range
        if from_key is not None:
            if from_inclusive:
                delete_range.range.from_key_inclusive = from_key
            else:
                delete_range.range.from_key_exclusive = from_key
        if to_key is not None:
            if to_inclusive:
                delete_range.range.to_key_inclusive = to_key
            else:
                delete_range.range.to_key_exclusive = to_key
        return request

    async def a_kv_delete_range(
        self, path, partition_id, from_key=None, to_key=None, from_inclusive=True, to_inclusive=False, version='v1'
    ):
        request = self.make_delete_range_request(path, partition_id, from_key, to_key, from_inclusive, to_inclusive)
        return await self.ainvoke(request, 'ExecuteTransaction', version)

    def make_read_request(self, path, partition_id, key, offset=None, size=None):
        request = keyvalue_api.ReadRequest()
        request.path = path
        request.partition_id = partition_id
        request.key = key
        if offset is not None:
            request.offset = offset
        if size is not None:
            request.size = size
        return request

    async def a_kv_read(self, path, partition_id, key, offset=None, size=None, version='v1'):
        request = self.make_read_request(path, partition_id, key, offset, size)
        return await self.ainvoke(request, 'Read', version)

    def create_tablets(self, number_of_tablets, path, binded_channels=None):
        request = keyvalue_api.CreateVolumeRequest()
        request.path = path
        request.partition_count = number_of_tablets
        if binded_channels:
            for media in binded_channels:
                new_channel = request.storage_config.channel.add()
                new_channel.media = media
        return self.invoke(request, 'CreateVolume', version='v1')

    def drop_tablets(self, path):
        request = keyvalue_api.DropVolumeRequest()
        request.path = path
        return self.invoke(request, 'DropVolume', version='v1')

    def close(self):
        self._channel.close()

    async def aclose(self):
        if self._async_channel is not None:
            await self._async_channel.close()

    def __del__(self):
        channel = getattr(self, '_channel', None)
        if channel is not None:
            channel.close()
