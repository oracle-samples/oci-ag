# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import os

import oci

from common.logger.logger import Logger


class BaseVCN:
    logger = Logger(__name__).get_logger()
    _signer_type = None
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
                self._signer = oci.auth.signers.InstancePrincipalsDelegationTokenSigner(delegation_token=token)

            else:
                self.logger.exception(
                "Please specify a valid OCI_AUTH_TYPE in config.ini. " \
                "Accepted values are 'security_token_file' and 'delegation_token_file'."
                )
                raise Exception("Specify a valid OCI_AUTH_TYPE in config.ini. " \
                "Accepted values are 'security_token_file' and 'delegation_token_file'.")
        else:
            self._signer = oci.auth.signers.get_resource_principals_signer()

    def __get_signer(self):
        if self._signer is None:
            self.__set_signer()
        return self._signer

    def __set_client(self):
        self.__client = oci.core.VirtualNetworkClient(
            config=self.__get_config(), signer=self.__get_signer()
        )

    def _get_client(self):
        if self.__client is None:
            self.__set_client()

        return self.__client


class DfaCreateVCN(BaseVCN):
    __service_id = None
    __vcn_id = None
    __nat_gateway_id = None
    __nat_route_table_id = None
    __security_list_ids = []
    __private_subnet_id = None
    __private_subnet_name = None
    __vcn_display_name = None

    def _set_vcn_display_name(self):
        self.__vcn_display_name = os.environ["RESOURCE_NAME_PREFIX"] + "_vcn"

    def __get_vcn_display_name(self):
        if self.__vcn_display_name is None:
            self._set_vcn_display_name()
        return self.__vcn_display_name

    def __get_vcn_id(self):
        return self.__vcn_id

    def __vcn_exists(self, vcn_display_name):
        list_vcns_response = self._get_client().list_vcns(
            compartment_id=os.environ["DFA_COMPARTMENT_ID"],
            display_name=vcn_display_name,
        )
        if len(list_vcns_response.data) > 0:
            self.__vcn_id = list_vcns_response.data[0].id
            return True
        return False

    def create_vcn(self):
        vcn_display_name = self.__get_vcn_display_name()
        if not self.__vcn_exists(vcn_display_name):
            create_vcn_response = self._get_client().create_vcn(
                create_vcn_details=oci.core.models.CreateVcnDetails(
                    compartment_id=os.environ["DFA_COMPARTMENT_ID"],
                    display_name=vcn_display_name,
                    cidr_block="10.0.0.0/16",
                    dns_label="dfavcn",
                    freeform_tags={"Feature": "Data Feed Analytics(DFA)"},
                )
            )
            self.__vcn_id = create_vcn_response.data.id
            self.logger.info("Successfully created the VCN %s", vcn_display_name)
        else:
            self.logger.info("VCN with the name %s already exists", vcn_display_name)
        return True

    def __set_service_id(self):
        self.find_service()

    def __get_service_id(self):
        if self.__service_id is None:
            self.__set_service_id()
        return self.__service_id

    def find_service(self):
        list_services_response = self._get_client().list_services(limit=10)
        target_service_name = (
            f'All {os.environ["DFA_REGION_KEY"]} Services In Oracle Services Network'
        )

        for service in list_services_response.data:
            service_name = service.name
            if target_service_name in service_name:
                self.__service_id = service.id

        if self.__service_id is None:
            raise ValueError(
                f"Could not find id for service: {target_service_name}. \
Please check ensure configurations are correct before proceeding."
            )

        self.logger.info("Successfully retrieved the service id for %s", target_service_name)

    def __service_gateway_exists(self, service_gateway_display_name):
        list_service_gateway_response = self._get_client().list_service_gateways(
            compartment_id=os.environ["DFA_COMPARTMENT_ID"], vcn_id=self.__get_vcn_id()
        )
        if len(list_service_gateway_response.data) > 0:
            for service_gateway in list_service_gateway_response.data:
                if service_gateway.display_name == service_gateway_display_name:
                    return True
        return False

    def create_service_gateway(self):
        service_gateway_display_name = os.environ["RESOURCE_NAME_PREFIX"] + "_service_gateway"
        if not self.__service_gateway_exists(service_gateway_display_name):
            self._get_client().create_service_gateway(
                create_service_gateway_details=oci.core.models.CreateServiceGatewayDetails(
                    compartment_id=os.environ["DFA_COMPARTMENT_ID"],
                    display_name=service_gateway_display_name,
                    services=[
                        oci.core.models.ServiceIdRequestDetails(service_id=self.__get_service_id())
                    ],
                    vcn_id=self.__get_vcn_id(),
                    freeform_tags={"Feature": "Data Feed Analytics(DFA)"},
                )
            )
            self.logger.info(
                "Successfully created the service gateway %s for the VCN",
                service_gateway_display_name,
            )
        else:
            self.logger.info(
                "Service gateway with the name %s already exists for this VCN",
                service_gateway_display_name,
            )
        return True

    def __nat_gateway_exists(self, nat_gateway_route_table):
        list_nat_gateway_response = self._get_client().list_nat_gateways(
            compartment_id=os.environ["DFA_COMPARTMENT_ID"],
            vcn_id=self.__get_vcn_id(),
            display_name=nat_gateway_route_table,
        )
        if len(list_nat_gateway_response.data) > 0:
            self.__nat_gateway_id = list_nat_gateway_response.data[0].id
            return True
        return False

    def create_nat_gateway(self):
        nat_gateway_display_name = os.environ["RESOURCE_NAME_PREFIX"] + "_nat_gateway"
        if not self.__nat_gateway_exists(nat_gateway_display_name):
            create_nat_gateway_details = self._get_client().create_nat_gateway(
                create_nat_gateway_details=oci.core.models.CreateNatGatewayDetails(
                    compartment_id=os.environ["DFA_COMPARTMENT_ID"],
                    display_name=nat_gateway_display_name,
                    vcn_id=self.__get_vcn_id(),
                    freeform_tags={"Feature": "Data Feed Analytics(DFA)"},
                )
            )
            self.__nat_gateway_id = create_nat_gateway_details.data.id
            self.logger.info(
                "Successfully created the nat gateway %s for the VCN", nat_gateway_display_name
            )
        else:
            self.logger.info(
                "NAT gateway with the name %s already exists for this VCN", nat_gateway_display_name
            )
        return True

    def __nat_route_table_exists(self, nat_route_table_display_name):
        list_route_table_response = self._get_client().list_route_tables(
            compartment_id=os.environ["DFA_COMPARTMENT_ID"],
            vcn_id=self.__get_vcn_id(),
            display_name=nat_route_table_display_name,
        )
        if len(list_route_table_response.data) > 0:
            self.__nat_route_table_id = list_route_table_response.data[0].id
            return True
        return False

    def create_nat_route_table(self):
        nat_route_table_display_name = os.environ["RESOURCE_NAME_PREFIX"] + " nat route table"
        if not self.__nat_route_table_exists(nat_route_table_display_name):
            create_route_table_response = self._get_client().create_route_table(
                create_route_table_details=oci.core.models.CreateRouteTableDetails(
                    compartment_id=os.environ["DFA_COMPARTMENT_ID"],
                    vcn_id=self.__get_vcn_id(),
                    display_name=nat_route_table_display_name,
                    route_rules=[
                        oci.core.models.RouteRule(
                            destination="0.0.0.0/0",
                            destination_type="CIDR_BLOCK",
                            route_type="STATIC",
                            network_entity_id=self.__nat_gateway_id,
                        )
                    ],
                    freeform_tags={"Feature": "Data Feed Analytics(DFA)"},
                )
            )
            self.__nat_route_table_id = create_route_table_response.data.id
            self.logger.info(
                "Successfully created the route table %s for the NAT gateway",
                nat_route_table_display_name,
            )
        else:
            self.logger.info(
                "Route table with the name %s already exists for this NAT gateway",
                nat_route_table_display_name,
            )
        return True

    def __get_security_lists(self):
        list_security_lists_response = self._get_client().list_security_lists(
            compartment_id=os.environ["DFA_COMPARTMENT_ID"],
            vcn_id=self.__get_vcn_id(),
        )
        if len(list_security_lists_response.data) > 0:
            for security_list in list_security_lists_response.data:
                self.__security_list_ids.append(security_list.id)
        self.logger.info(self.__security_list_ids)
        return False

    def get_security_list_ids(self):
        if len(self.__security_list_ids) > 0:
            self.__get_security_lists()
        return self.__security_list_ids

    def __set_private_subnet_name(self):
        self.__private_subnet_name = os.environ["RESOURCE_NAME_PREFIX"] + "_private_subnet"

    def get_private_subnet_name(self):
        if self.__private_subnet_name is None:
            self.__set_private_subnet_name()
        return self.__private_subnet_name

    def get_private_subnet_id(self):
        if self.__private_subnet_id is None:
            self.__private_subnet_exists(self.get_private_subnet_name())
        return self.__private_subnet_id

    def __private_subnet_exists(self, private_subnet_display_name):
        list_subnets_response = self._get_client().list_subnets(
            compartment_id=os.environ["DFA_COMPARTMENT_ID"],
            display_name=private_subnet_display_name,
            vcn_id=self.__get_vcn_id(),
        )
        if len(list_subnets_response.data) > 0:
            self.__private_subnet_id = list_subnets_response.data[0].id
            return True
        return False

    def create_private_subnet(self):
        private_subnet_display_name = self.get_private_subnet_name()
        if not self.__private_subnet_exists(private_subnet_display_name):
            create_subnet_response = self._get_client().create_subnet(
                create_subnet_details=oci.core.models.CreateSubnetDetails(
                    cidr_block="10.0.0.0/16",
                    compartment_id=os.environ["DFA_COMPARTMENT_ID"],
                    vcn_id=self.__get_vcn_id(),
                    display_name=private_subnet_display_name,
                    prohibit_public_ip_on_vnic=True,
                    dns_label="privatesubnet",
                    security_list_ids=self.get_security_list_ids(),
                    route_table_id=self.__nat_route_table_id,
                    freeform_tags={"Feature": "Data Feed Analytics(DFA)"},
                )
            )
            self.__private_subnet_id = create_subnet_response.data.id
            self.logger.info(
                "Successfully created the private subnet %s for the VCN",
                private_subnet_display_name,
            )
        else:
            self.logger.info(
                "Private subnet with the name %s already exists for this VCN",
                private_subnet_display_name,
            )
        return True
