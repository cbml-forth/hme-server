#!/bin/sh

now=`date "+%s"`
TOKEN_FILE=$HOME/.chic_saml
if [ -f $TOKEN_FILE  ]; then
    eval $(stat -s $TOKEN_FILE)
    t=`echo "($now - $st_mtime) / 60" | bc`
    if [ $t -gt 1200  ]; then
        chic_login
    fi
else
    chic_login
fi

for i in `seq 1283 1344`;
do
	echo DELETING Experiment $i
	chic_curl -XDELETE https://hf-dev-chic.ics.forth.gr:8000/api/director/workflowlist/$i/
done
