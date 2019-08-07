## GTT - API

### Trending Pages

Endpoint: [https://githubtr.com/trending/](https://githubtr.com/trending/) 
follow by `some_language_code` like `c`, `python`, the full list see page indexes below.

Parameter: `since` could be `daily`, `weekly`, and `monthly`.

Example: [https://githubtr.com/trending/c?since=weekly](https://githubtr.com/trending/c?since=weekly).

Example: [https://githubtr.com/trending/python?since=monthly](https://githubtr.com/trending/python?since=monthly).

Page index by name: [for human](https://www.githubtr.com/api/name/), [for machine](https://www.githubtr.com/api/name/?type=json).

Page index by last update time: [for human](https://www.githubtr.com/api/time/), [for machine](https://www.githubtr.com/api/time/?type=json).


### Search a Repo (whether got onto trending)

Endpoint: [https://githubtr.com/search/](https://githubtr.com/search/).

Parameter: `q` could be some developer's name or repository's name (cases does not matter).

Parameter: `type` could be `json` or empty

Example: [https://githubtr.com/search/?q=microsoft](https://githubtr.com/search/?q=microsoft)

Example: [https://githubtr.com/search/?q=calculator&type=json](https://githubtr.com/search/?q=calculator&type=json)


### Search a Repo's History (what ranks it had)

Endpoint: [https://githubtr.com/record/](https://githubtr.com/record/).

Parameter: `a`: author/developer's name, use the result of a `/search` endpoint search.

Parameter: `r`: repository's name, the same as above

Parameter: `c`: the code of a programming language, the same as above

Parameter: `d`: the date range of a programming language, the same as above

Parameter: `type` could be `json` or empty

Example: [https://githubtr.com/record/?a=microsoft&r=customer-scripts&c=powershell&d=daily](https://www.githubtr.com/record/?a=microsoft&r=customer-scripts&c=powershell&d=daily)

Example: [https://www.githubtr.com/record/?a=facebook&r=zstd&c=c&d=monthly&type=json](https://www.githubtr.com/record/?a=facebook&r=zstd&c=c&d=monthly&type=json)
