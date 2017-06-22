#!/bin/bash
while echo -n "> "; IFS='$\n' read -r line; do
    filename=$(grep -r -m 1 -l -- "$line" * | head -n 1)
    if [[ ! -z "$filename" ]]; then
        echo "$filename"
        vim "$filename"
    fi
done
echo
