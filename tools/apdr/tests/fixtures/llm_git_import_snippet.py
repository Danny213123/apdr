#!/usr/bin/env python
"""Uses 'import git' which maps to the GitPython package on PyPI."""
import git
from time import localtime, strftime
from subprocess import call

repo = git.Repo("/path/to/repo")
for commit in repo.iter_commits("master", max_count=10):
    print(strftime("%Y-%m-%d %H:%M:%S", localtime(commit.committed_date)),
          commit.hexsha[:7], commit.message.strip())
