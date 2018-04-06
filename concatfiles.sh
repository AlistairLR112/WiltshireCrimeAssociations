rm wiltshirepolice.csv
touch wiltshirepolice.csv
FILES="/Users/arogers2/Documents/Police/*/*.csv"
OUTPUT="wiltshirepolice.csv"
i=0
for filename in $FILES; do 
if [ "$filename"  != "$OUTPUT" ] ;      # Avoid recursion 
 then 
   if [[ $i -eq 0 ]] ; then 
      head -1  $filename >   $OUTPUT # Copy header if it is the first file
   fi
   tail -n +2  $filename >>  $OUTPUT # Append from the 2nd line each file
   i=$(( $i + 1 ))                        # Increase the counter
 fi
done
