#!/bin/bash

# Check if there is any inefficient assert function usage in package.

res=$(grep -rn --include=\*_test.go -E "(re|suite|require)\.(True|False)\((t, )?reflect\.DeepEqual\(" . | sort -u) \

if [ "$res" ]; then
  echo "following packages use the inefficient assert function: please replace reflect.DeepEqual with require.Equal"
  echo "$res"
  exit 1
fi

res=$(grep -rn --include=\*_test.go -E "(re|suite|require)\.(True|False)\((t, )?strings\.Contains\(" . | sort -u)

if [ "$res" ]; then
  echo "following packages use the inefficient assert function: please replace strings.Contains with require.Contains"
  echo "$res"
  exit 1
fi

res=$(grep -rn --include=\*_test.go -E "(re|suite|require)\.(Nil|NotNil)\((t, )?(err|error)" . | sort -u)

if [ "$res" ]; then
  echo "following packages use the inefficient assert function: please replace require.Nil/NotNil with require.NoError/Error"
  echo "$res"
  exit 1
fi

res=$(grep -rn --include=\*_test.go -E "(re|suite|require)\.(Equal|NotEqual)\((t, )?(true|false)" . | sort -u)

if [ "$res" ]; then
  echo "following packages use the inefficient assert function: please replace require.Equal/NotEqual(true, xxx) with require.True/False"
  echo "$res"
  exit 1
fi

exit 0

# add new line to trigger ci
