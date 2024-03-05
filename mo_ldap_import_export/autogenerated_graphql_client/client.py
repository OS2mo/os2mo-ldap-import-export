from .async_base_client import AsyncBaseClient
from .create_it_system import CreateItSystem
from .create_it_system import CreateItSystemItsystemCreate
from .input_types import ITSystemCreateInput
from .read_facet_uuid import ReadFacetUuid
from .read_facet_uuid import ReadFacetUuidFacets


def gql(q: str) -> str:
    return q


class GraphQLClient(AsyncBaseClient):
    async def create_it_system(
        self, input: ITSystemCreateInput
    ) -> CreateItSystemItsystemCreate:
        query = gql(
            """
            mutation create_it_system($input: ITSystemCreateInput!) {
              itsystem_create(input: $input) {
                uuid
              }
            }
            """
        )
        variables: dict[str, object] = {"input": input}
        response = await self.execute(query=query, variables=variables)
        data = self.get_data(response)
        return CreateItSystem.parse_obj(data).itsystem_create

    async def read_facet_uuid(self, user_key: str) -> ReadFacetUuidFacets:
        query = gql(
            """
            query read_facet_uuid($user_key: String!) {
              facets(filter: {user_keys: [$user_key]}) {
                objects {
                  current {
                    uuid
                  }
                }
              }
            }
            """
        )
        variables: dict[str, object] = {"user_key": user_key}
        response = await self.execute(query=query, variables=variables)
        data = self.get_data(response)
        return ReadFacetUuid.parse_obj(data).facets
