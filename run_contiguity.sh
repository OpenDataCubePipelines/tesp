##################################################################################################################
# Run Contiguity - creates an 8bit contiguity mask for all products in input dir                                 #
# ./run_contiguity.sh <input product path> <output product path>                                                 #
#                                                                                                                #
# usage example:                                                                                                 #
# ./run_contiguity.sh /g/data/v10/testing_ground/S2A-samples/s2-for-packaging /g/data/v10/tmp/s2_ard_vrt NBART   #
#                                                                                                                #
##################################################################################################################

cwd=$PWD

for i in `ls $1`; do for j in `ls $1/$i`; do k=`find $1/$i/$j/$3/ -wholename '*TIF' | grep $3`; gdalbuildvrt -resolution user -tr 20 20 -separate -overwrite `echo $2/$j\_ALLBANDS_20m.vrt | sed -e "s/ARD/$3/g"` $k; done; done

for l in `find $2 -name "*$3_*vrt"`; do python contiguity.py $l --output $2/;  done

for i in `ls $1`; do for j in `ls $1/$i`; do cd $1/$i/$j/$3; gdalbuildvrt -separate -overwrite `echo $j\_BANDS_10m.vrt | sed -e "s/ARD/$3/g"` *_B0[2-48].TIF; gdalbuildvrt -separate -overwrite `echo $j\_BANDS_20m.vrt | sed -e "s/ARD/$3/g"` *_B0[5-7].TIF *_B8A.TIF *_B1[0-1].TIF; gdalbuildvrt -separate -overwrite `echo $j\_BANDS_60m.vrt | sed -e "s/ARD/$3/g"` *_B01.TIF *_B09.TIF; cd $cwd; done; done

# below for non gurus :)

# for i in `ls $1`
#     do for j in `ls $1/$i`
#         do k=`find $1/$i/$j/$3/ -wholename '*TIF'`
#         gdalbuildvrt -resolution user -tr 20 20 -separate -overwrite `echo $2/$j\_ALLBANDS_20m.vrt | sed -e "s/ARD/$3/g"` $k
#     done
# done

# for l in `find $2 -name "*$3_*vrt"`
#     do python contiguity.py $l --output $2/
# done
