import asyncio
from github_client import AsyncGithubClient
from gql import Client
from gql.transport.aiohttp import AIOHTTPTransport
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from os import getenv

load_dotenv()

TOKEN = getenv("TOKEN")


async def main():
    transport = AIOHTTPTransport(url="https://api.github.com/graphql", headers={"Authorization": f"Bearer {TOKEN}"})
    client = Client(transport=transport, fetch_schema_from_transport=True)
    author_to_commits_count = {}

    async with client as session:
        gh_client = AsyncGithubClient(session)
        commits_authors = await gh_client.get_org_commits_authors("twitter")
        for author in commits_authors:
            if author in author_to_commits_count.keys():
                author_to_commits_count[author] += 1
            else:
                author_to_commits_count[author] = 1

    authors = []
    commits_count = []
    for pair in sorted(author_to_commits_count.items(), key=lambda x: -x[1]):
        authors.append(pair[0])
        commits_count.append(pair[1])

    print(f'Топ {min(len(authors), 20)} самых активных авторов коммитов на GitHub в организации twitter')
    for i in range(min(len(authors), 20)):
        print(str(i + 1) + ". " + authors[i] + ": " + str(commits_count[i]))

    plt.figure(figsize=(10, 6))
    plt.barh(authors[: min(len(authors), 20)][::-1], commits_count[: min(len(commits_count), 20)][::-1], color='skyblue')

    plt.title(f'Топ {min(len(authors), 20)} самых активных авторов коммитов на GitHub в организации twitter')
    plt.xlabel('Количество коммитов')
    plt.ylabel('Автор')

    plt.tight_layout()
    plt.show()


asyncio.run(main())
