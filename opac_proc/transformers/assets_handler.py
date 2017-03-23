# coding: utf-8

import os
import time
from datetime import datetime

from opac_ssm_api.client import Client

from opac_proc.web import config


cli = Client(config.OPAC_SSM_GRPC_SERVER_HOST_CLI, config.OPAC_SSM_GRPC_SERVER_PORT)


def client_status():
    ret, message = cli.get_asset('none')
    if message.get('error_message') == 'Exception calling application: badly formed hexadecimal UUID string':
        return True
    else:
        if not isinstance(message, dict):
            message = {'message': message}
        message.update({'client': [config.OPAC_SSM_GRPC_SERVER_HOST_CLI, config.OPAC_SSM_GRPC_SERVER_PORT]})

        return message
    return False


def now():
    return datetime.now().isoformat()


class Asset(object):

    def __init__(self, pfile, filename, filetype, metadata, bucket_name):
        self.pfile = pfile
        self.filetype = filetype
        self.metadata = metadata
        self.bucket_name = bucket_name
        self.name = filename
        self.ID = None
        self.registered_url = None
        self.error_message = None
        self._status = None
        
    def register(self):
        self._status = None
        self.error_message = None
        self.ID = None
        ssm_status = client_status()
        if ssm_status is True:
            if self.pfile is None:
                self._status = 'error'
                self.error_message = {'error message': u'Valor inválido de arquivo para registrar em SSM'}
            self._status = 'queued'
            self.ID = cli.add_asset(self.pfile, self.name, self.filetype, self.metadata, self.bucket_name)
        else:
            self._status = 'error'
            self.error_message = ssm_status
        return ssm_status

    def wait_registration(self):
        while self.status == 'queued':
            time.sleep(10)

    @property
    def status(self):
        if self._status == 'queued':
            if cli.get_task_state(self.ID) in ['SUCESS', 'SUCCESS']:
                self._status = 'registered'
        return self._status

    @property
    def data(self):
        if self.status == 'error':
            return self.error_message
        if self.status == 'queued':
            self.wait_registration()
        if self.status == 'registered':
            result, data = cli.get_asset_info(self.ID)
            if result is True:
                return data
