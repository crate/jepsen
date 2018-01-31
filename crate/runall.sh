#! /bin/sh

EXIT_STATUS=0
STATUS=""
for test in "dirty-read" "lost-updates" "version-divergence"
do
    lein run test --test ${test} --concurrency 2n --tarball $1
    TEST_EXIT_STATUS=$?
    EXIT_STATUS=$(expr ${EXIT_STATUS} + ${TEST_EXIT_STATUS})
    STATUS="$STATUS \n Test ${test} exited with status ${TEST_EXIT_STATUS}"
    sleep 15
    for i in $(seq 1 5)
    do
        nodeName="n$i"
        test_dir=$(ls -d store/[${test}]*)
        latest_test_dir=$(ls -Art store/${test}* | tail -n 1)
        scp root@"$nodeName":/var/log/auth.log "${test_dir}/${latest_test_dir}/${nodeName}_auth.out"
        scp root@"$nodeName":/var/log/syslog "${test_dir}/${latest_test_dir}/${nodeName}_syslog"
    done
done

echo $STATUS
exit ${EXIT_STATUS}
