# for i in {42001..42133}
# do
# if (( i % 2 )) ; then
# curl -L 'https://www2.census.gov/cgi-bin/pvs_zipbatch_download' \
#   -H 'Content-Type: application/x-www-form-urlencoded' \
#   --data-raw "county1=$i&year=18&vint=v2&state=42" \
#   -o census/$i.zip
# fi
# done
#
for i in {42001..42133}
do
if (( i % 2 )) ; then
curl -L "https://www2.census.gov/geo/tiger/TIGER2020PL/STATE/42_PENNSYLVANIA/42091/tl_2020_"$i"_vtd20.zip" -o "census/tl_2020_"$i"_vtd20.zip"
fi
done
curl -L https://www2.census.gov/geo/pvs/tiger2010st/42_Pennsylvania/42/tl_2010_42_bg10.zip
