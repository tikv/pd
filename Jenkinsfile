#!groovy

node {
    def TIDB_TEST_BRANCH = "master"
    def TIDB_BRANCH = "rc3"
    def TIKV_BRANCH = "rc3"

    fileLoader.withGit('git@github.com:pingcap/SRE.git', 'master', 'github-iamxy-ssh', '') {
        fileLoader.load('jenkins/ci/pingcap_pd_branch.groovy').call(TIDB_TEST_BRANCH, TIDB_BRANCH, TIKV_BRANCH)
    }
}
