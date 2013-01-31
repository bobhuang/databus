#!/bin/bash

if gradle -Dopen_source=true clean cleanEclipse ; then
	echo "clean PASSED."
else
	exit 1
fi

if gradle -Dopen_source=true assemble ; then
	echo "assemble PASSED."
else
	exit 1
fi
if gradle -Dopen_source=true eclipse ; then
	echo "eclipse PASSED."
else
	exit 1
fi
if gradle -Dopen_source=true test ; then
	echo "test PASSED."
else
	exit 1
fi 
