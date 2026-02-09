from typing import Any
from typing import Optional
from uuid import UUID

from ._testing__create_employee import TestingCreateEmployee
from ._testing__create_employee import TestingCreateEmployeeEmployeeCreate
from ._testing__get_employee import TestingGetEmployee
from ._testing__get_employee import TestingGetEmployeeEmployees
from ._testing__send_event import TestingSendEvent
from .acknowledge_event import AcknowledgeEvent
from .async_base_client import AsyncBaseClient
from .declare_event_listener import DeclareEventListener
from .declare_event_listener import DeclareEventListenerEventListenerDeclare
from .declare_event_namespace import DeclareEventNamespace
from .declare_event_namespace import DeclareEventNamespaceEventNamespaceDeclare
from .fetch_event import FetchEvent
from .fetch_event import FetchEventEventFetch
from .input_types import EventSendInput
from .input_types import ListenerCreateInput
from .input_types import NamespaceCreateInput


def gql(q: str) -> str:
    return q


class GraphQLClient(AsyncBaseClient):
    async def declare_event_namespace(
        self, input: NamespaceCreateInput
    ) -> DeclareEventNamespaceEventNamespaceDeclare:
        query = gql(
            """
            mutation DeclareEventNamespace($input: NamespaceCreateInput!) {
              event_namespace_declare(input: $input) {
                name
              }
            }
            """
        )
        variables: dict[str, object] = {"input": input}
        response = await self.execute(query=query, variables=variables)
        data = self.get_data(response)
        return DeclareEventNamespace.parse_obj(data).event_namespace_declare

    async def declare_event_listener(
        self, input: ListenerCreateInput
    ) -> DeclareEventListenerEventListenerDeclare:
        query = gql(
            """
            mutation DeclareEventListener($input: ListenerCreateInput!) {
              event_listener_declare(input: $input) {
                uuid
              }
            }
            """
        )
        variables: dict[str, object] = {"input": input}
        response = await self.execute(query=query, variables=variables)
        data = self.get_data(response)
        return DeclareEventListener.parse_obj(data).event_listener_declare

    async def fetch_event(self, listener: UUID) -> Optional[FetchEventEventFetch]:
        query = gql(
            """
            query FetchEvent($listener: UUID!) {
              event_fetch(filter: {listener: $listener}) {
                subject
                priority
                token
              }
            }
            """
        )
        variables: dict[str, object] = {"listener": listener}
        response = await self.execute(query=query, variables=variables)
        data = self.get_data(response)
        return FetchEvent.parse_obj(data).event_fetch

    async def acknowledge_event(self, token: Any) -> bool:
        query = gql(
            """
            mutation AcknowledgeEvent($token: EventToken!) {
              event_acknowledge(input: {token: $token})
            }
            """
        )
        variables: dict[str, object] = {"token": token}
        response = await self.execute(query=query, variables=variables)
        data = self.get_data(response)
        return AcknowledgeEvent.parse_obj(data).event_acknowledge

    async def _testing__send_event(self, input: EventSendInput) -> bool:
        query = gql(
            """
            mutation _Testing_SendEvent($input: EventSendInput!) {
              event_send(input: $input)
            }
            """
        )
        variables: dict[str, object] = {"input": input}
        response = await self.execute(query=query, variables=variables)
        data = self.get_data(response)
        return TestingSendEvent.parse_obj(data).event_send

    async def _testing__get_employee(
        self, cpr_number: Any
    ) -> TestingGetEmployeeEmployees:
        query = gql(
            """
            query _Testing_GetEmployee($cpr_number: CPR!) {
              employees(filter: {cpr_numbers: [$cpr_number]}) {
                objects {
                  validities {
                    uuid
                    cpr_number
                    given_name
                  }
                }
              }
            }
            """
        )
        variables: dict[str, object] = {"cpr_number": cpr_number}
        response = await self.execute(query=query, variables=variables)
        data = self.get_data(response)
        return TestingGetEmployee.parse_obj(data).employees

    async def _testing__create_employee(
        self, cpr_number: Any
    ) -> TestingCreateEmployeeEmployeeCreate:
        query = gql(
            """
            mutation _Testing_CreateEmployee($cpr_number: CPR!) {
              employee_create(
                input: {given_name: "Alice", surname: "Nielsen", cpr_number: $cpr_number}
              ) {
                validities {
                  uuid
                }
              }
            }
            """
        )
        variables: dict[str, object] = {"cpr_number": cpr_number}
        response = await self.execute(query=query, variables=variables)
        data = self.get_data(response)
        return TestingCreateEmployee.parse_obj(data).employee_create
