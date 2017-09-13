# abbs-meta
Generate a metadata SQLite database from an ABBS tree

```
usage: abbsmeta.py [-h] [-p PATH] [-m PATH] [-d FILE] [-b BRANCHES]
                   [-B BRANCH] [-c CATEGORY] [-u URL] [-P PRIORITY] [-v]
                   [--no-sync] [--reset]
                   name

Generate metadata database for abbs trees.

positional arguments:
  name                  Repository / abbs tree name

optional arguments:
  -h, --help            show this help message and exit
  -p PATH, --basepath PATH
                        Directory with both Git and Fossil repositories
  -m PATH, --markpath PATH
                        Directory with Git and Fossil sync marks
  -d FILE, --dbfile FILE
                        Abbs meta database file
  -b BRANCHES, --branches BRANCHES
                        Branches to consider, seperated by comma (,)
  -B BRANCH, --mainbranch BRANCH
                        Git repo main branch name
  -c CATEGORY, --category CATEGORY
                        Category, 'base' or 'bsp'
  -u URL, --url URL     Repo url
  -P PRIORITY, --priority PRIORITY
                        Priority to consider
  -v, --verbose         Show debug logs
  --no-sync             Don't sync Git and Fossil repos
  --reset               Reset sync status
```
