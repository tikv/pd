# Development Workflow

Start by forking the pd GitHub repository, make changes in a branch and then send a pull request. 

### Setup your pd GitHub Repository
Fork [pd upstream](https://github.com/pingcap/pd/fork) source repository to your own personal repository.

```sh
$ mkdir -p $GOPATH/src/github.com/pingcap
$ cd $GOPATH/src/github.com/pingcap
$ git clone < your personal forked pd repo>
$ cd pd
```

### Set up git remote as ``upstream``
```sh
$ cd $GOPATH/src/github.com/pingcap/pd
$ git remote add upstream https://github.com/pingcap/pd
$ git fetch upstream
$ git merge upstream/master
...
```

### Create your feature branch
Before making code changes, make sure you create a separate branch for these changes

```
$ git checkout -b my-feature
```

### Test pd server changes
After your code changes, make sure

- To add test cases for the new code.
- To run `make test`.


### Commit changes
After verification, commit your changes. 

```
$ git commit -am 'information about your feature'
```

### Push to the branch
Push your locally committed changes to the remote origin (your fork)
```
$ git push origin my-feature
```

### Create a Pull Request
Pull requests can be created via GitHub. Refer to [this document](https://help.github.com/articles/creating-a-pull-request/) for detailed steps on how to create a pull request.
