import asyncio
from gql import gql
from gql.client import AsyncClientSession, ReconnectingAsyncClientSession
from gql.transport.exceptions import TransportServerError

semaphore = asyncio.Semaphore(40)


class RequestsRemainingZeroError(Exception):
    def __str__(self):
        return "The remaining number of requests is 0"


class BadResponseException(Exception):
    def __str__(self):
        return "Got some problem with the response"


class AsyncGithubClient:
    _repos_limit = 100
    _refs_limit = 100
    _commits_limit = 100

    def __init__(self, async_session: AsyncClientSession | ReconnectingAsyncClientSession):
        self._async_session = async_session
        self._requests_remaining_cached = None

    @staticmethod
    def _response_is_ok(response: dict) -> bool:
        return "error" not in response.keys()

    @property
    async def _requests_remaining(self) -> int:
        if not self._requests_remaining_cached:
            query = gql('''
                        {
                          rateLimit {
                            remaining
                          }
                        }
                        ''')
            response = await self._async_session.execute(query)
            try:
                self._requests_remaining_cached = int(response["rateLimit"]["remaining"])
            except (ValueError, KeyError):
                self._requests_remaining_cached = 0
        return self._requests_remaining_cached

    @_requests_remaining.setter
    def _requests_remaining(self, value: int):
        self._requests_remaining_cached = value

    async def get_org_repos_names(self, organization_login: str) -> list[str]:
        async with semaphore:
            if await self._requests_remaining < 1:
                raise RequestsRemainingZeroError()
            after = ""
            has_next_page = True
            res = []
            while has_next_page:
                query = gql(f'''
                    {{
                      organization(login: "{organization_login}") {{
                        repositories(first: {self._repos_limit}{(", after: " + '"' + after + '"') if after else ""}) {{
                          nodes {{
                              name
                          }}
                          pageInfo {{
                            endCursor
                            hasNextPage
                          }}
                        }}
                      }}
                    }}
                    ''')
                if await self._requests_remaining < 1:
                    raise RequestsRemainingZeroError()
                try:
                    response = await self._async_session.execute(query)
                except TransportServerError:
                    continue
                self._requests_remaining = await self._requests_remaining - 1
                if not self._response_is_ok(response):
                    raise BadResponseException()
                after = response["organization"]["repositories"]["pageInfo"]["endCursor"]
                has_next_page = bool(response["organization"]["repositories"]["pageInfo"]["hasNextPage"])
                res.extend(list(map(lambda x: list(x.values())[0], response["organization"]["repositories"]["nodes"])))
            return res

    async def get_repos_heads(self, organization_login: str, repos_name: str) -> list[str]:
        async with semaphore:
            after = ""
            has_next_page = True
            res = []
            while has_next_page:
                if await self._requests_remaining < 1:
                    raise RequestsRemainingZeroError()
                query = gql(f'''
                            {{
                              repository(owner: "{organization_login}", name: "{repos_name}") {{
                                refs(refPrefix: "refs/heads/", first: {self._repos_limit}{(", after: " + '"' + after
                                                                                           + '"') if after else ""}) {{
                                  nodes {{
                                    name
                                  }}
                                  pageInfo {{
                                    endCursor
                                    hasNextPage
                                  }}
                                }}
                              }}
                            }}
                            ''')
                try:
                    response = await self._async_session.execute(query)
                except TransportServerError:
                    continue
                self._requests_remaining = await self._requests_remaining - 1
                if not self._response_is_ok(response):
                    raise BadResponseException()
                after = response["repository"]["refs"]["pageInfo"]["endCursor"]
                has_next_page = bool(response["repository"]["refs"]["pageInfo"]["hasNextPage"])
                res.extend(list(map(lambda x: list(x.values())[0], response["repository"]["refs"]["nodes"])))
            return res

    async def get_head_commits_authors(self, organization_login: str, repos_name: str, head_name: str,
                                       already_been_commits_hashes: list[str]) -> list[str]:
        async with semaphore:
            after = ""
            has_next_page = True
            res = []
            commit_already_been = False
            while has_next_page:
                if commit_already_been:
                    break
                if await self._requests_remaining < 1:
                    raise RequestsRemainingZeroError()
                query_text = f'''
                {{
                  repository(owner: "{organization_login}", name: "{repos_name}") {{
                    ref(qualifiedName: "{head_name}") {{
                      target {{
                        ... on Commit {{
                          history(first: {self._commits_limit}{(", after: " + '"' + after + '"') if after else ""}) {{
                            nodes {{
                              oid
                              author {{
                                name
                                email
                              }}
                              parents {{
                                totalCount
                              }}
                            }}
                            pageInfo {{
                              hasNextPage
                              endCursor
                            }}
                          }}
                        }}
                      }}
                    }}
                  }}
                }}
                '''
                query = gql(query_text)
                try:
                    response = await self._async_session.execute(query)
                except TransportServerError:
                    continue
                if not self._response_is_ok(response):
                    raise BadResponseException()
                after = response["repository"]["ref"]["target"]["history"]["pageInfo"]["endCursor"]
                has_next_page = bool(response["repository"]["ref"]["target"]["history"]["pageInfo"]["hasNextPage"])
                for commit in response["repository"]["ref"]["target"]["history"]["nodes"]:
                    commit_hash = commit["oid"]
                    if commit_hash in already_been_commits_hashes:
                        commit_already_been = True
                        break
                    already_been_commits_hashes.append(commit_hash)
                    commit_parents_count = commit["parents"]["totalCount"]
                    if commit_parents_count > 1:
                        continue
                    res.append(commit["author"]["name"] + ", " + commit["author"]["email"])
            return res

    async def get_repos_commits_authors(self, organization_login: str, repos_name: str) -> list[str]:
        tasks = []
        res = []
        already_been_commits_hashes = []
        for head_name in await self.get_repos_heads(organization_login, repos_name):
            tasks.append(asyncio.create_task(self.get_head_commits_authors(organization_login, repos_name,
                                                                           head_name, already_been_commits_hashes)))
        results = await asyncio.gather(*tasks)
        for result in results:
            res.extend(result)
        return res

    async def get_org_commits_authors(self, organization_login: str) -> list[str]:
        tasks = []
        res = []
        for repos_name in await self.get_org_repos_names(organization_login):
            tasks.append(asyncio.create_task(self.get_repos_commits_authors(organization_login, repos_name)))
        results = await asyncio.gather(*tasks)
        for result in results:
            res.extend(result)
        return res
