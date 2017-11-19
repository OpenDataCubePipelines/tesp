##################################################################################################################
# Run Contiguity - creates an 8bit contiguity mask for all products in input dir                                 #
# ./run_contiguity.sh <input product path> <output product path>                                                 #
#                                                                                                                #
# usage example:                                                                                                 #
# ./run_contiguity.sh /g/data/v10/testing_ground/S2A-samples/s2-for-packaging /g/data/v10/tmp/s2_ard_vrt NBART   #
#                                                                                                                #
##################################################################################################################

for i in `ls $1`; do for j in `ls $1/$i`; do k=`find $1/$i/$j -name 'BAND*tif' | grep $3`; gdalbuildvrt -resolution user -tr 20 20 -separate -overwrite `echo $2/$j.vrt | sed -e "s/L1C/$3/g"` $k; done; done

for l in `find $2 -name *NBART*vrt`; do python contiguity.py $l --output $2/;  done
