# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import os

import oci

from common.logger.logger import Logger


class BaseArtifact:
    logger = Logger(__name__).get_logger()
    _signer = None
    __config = None
    __client = None

    def __set_config(self):
        if os.environ["DFA_SIGNER_TYPE"] == "user":
            self.__config = oci.config.from_file(
                os.environ["DFA_CONFIG_LOCATION"], os.environ["DFA_CONFIG_PROFILE"]
            )
        else:
            self.__config = {}

    def __get_config(self):
        if self.__config is None:
            self.__set_config()

        return self.__config

    def __set_signer(self):
        token = None

        if os.environ["DFA_SIGNER_TYPE"] == "user":
            if os.environ["OCI_AUTH_TYPE"] == "security_token_file":
                config = self.__get_config()
                token_file = config["security_token_file"]

                with open(token_file, "r", encoding="utf-8") as f:
                    token = f.read()

                private_key = oci.signer.load_private_key_from_file(config["key_file"])
                self._signer = oci.auth.signers.SecurityTokenSigner(token, private_key)

            elif os.environ["OCI_AUTH_TYPE"] == "delegation_token_file":
                config = self.__get_config()
                token_file = config["delegation_token_file"]
                with open(token_file, "r", encoding="utf-8") as f:
                    token = f.read()
                self._signer = oci.auth.signers.InstancePrincipalsDelegationTokenSigner(
                    delegation_token=token
                )

            else:
                self.logger.exception(
                    "Please specify a valid OCI_AUTH_TYPE in config.ini. "
                    "Accepted values are 'security_token_file' and 'delegation_token_file'."
                )
                raise Exception(
                    "Specify a valid OCI_AUTH_TYPE in config.ini. "
                    "Accepted values are 'security_token_file' and 'delegation_token_file'."
                )
        else:
            self._signer = oci.auth.signers.get_resource_principals_signer()

    def __get_signer(self):
        if self._signer is None:
            self.__set_signer()

        return self._signer

    def __set_client(self):
        self.__client = oci.artifacts.ArtifactsClient(
            config=self.__get_config(), signer=self.__get_signer()
        )

    def _get_client(self):
        if self.__client is None:
            self.__set_client()

        return self.__client


class DfaTransformerArtifacts(BaseArtifact):
    def get_latest_image(self):
        images = self.__get_image_by_repository_name_and_version()

        if not hasattr(images, "items") or len(images.items) == 0:
            raise Exception("No image found for transformer")

        return images.items[0]

    def __get_image_by_repository_name_and_version(self):
        list_container_images_response = self._get_client().list_container_images(
            compartment_id=os.environ["DFA_COMPARTMENT_ID"],
            compartment_id_in_subtree=False,
            repository_name=os.environ["REPOSITORY_NAME"],
            version=os.environ["IMAGE_VERSION"],
        )

        return list_container_images_response.data
