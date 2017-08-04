#!groovy

node {
    def TIDB_TEST_BRANCH = "master"
    def TIDB_BRANCH = "rc4"
    def TIKV_BRANCH = "rc4"

    fileLoader.withGit('git@github.com:pingcap/SRE.git', 'master', 'github-iamxy-ssh', '') {
        fileLoader.load('jenkins/ci/pingcap_pd_branch.groovy').call(TIDB_TEST_BRANCH, TIDB_BRANCH, TIKV_BRANCH)
    }
}
