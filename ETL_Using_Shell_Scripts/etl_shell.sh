#The command below translates all lower case alphabets to upper case.
echo "Shell Scripting" | tr "[a-z]" "[A-Z]"

#You could also use the pre-defined character sets also for this purpose:
echo "Shell Scripting" | tr "[:lower:]" "[:upper:]"


#The command below translates all upper case alphabets to lower case.
echo "Shell Scripting" | tr  "[A-Z]" "[a-z]"

#The command below replaces repeat occurrences of ‘space’ in the output of ps command with one ‘space’.
ps | tr -s " "

#The command below deletes all digits.
echo "My login pin is 5634" | tr -d "[:digit:]"


#The command below shows how to extract the first four characters.
echo "database" | cut -c1-4

#The command below shows how to extract 5th to 8th characters.
echo "database" | cut -c1,5

#The command below extracts usernames (the first field) from /etc/passwd.
cut -d":" -f1 /etc/passwdps | tr -s " "