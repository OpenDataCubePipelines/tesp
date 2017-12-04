##################################################################################################################
# Run Contiguity - creates an 8bit contiguity mask for all products in input dir                                 #
# ./run_contiguity.sh <input product path> <output product path>                                                 #
#                                                                                                                #
# usage example:                                                                                                 #
# ./run_contiguity.sh /g/data/v10/testing_ground/S2A-samples/s2-for-packaging /g/data/v10/tmp/s2_ard_vrt NBART   #
#                                                                                                                #
##################################################################################################################

cwd=$PWD
contiguity=$cwd/contiguity.py
contrast=$cwd/contrast.py

# for i in `ls $1`; do for j in `ls $1/$i`; do k=`find $1/$i/$j/$3/ -wholename '*TIF' | grep $3`; gdalbuildvrt -resolution user -tr 20 20 -separate -overwrite `echo $2/$j\_ALLBANDS_20m.vrt | sed -e "s/ARD/$3/g"` $k; done; done

# for l in `find $2 -name "*$3_*vrt"`; do python contiguity.py $l --output $2/;  done

# for i in `ls $1`; do for j in `ls $1/$i`; do cd $1/$i/$j/$3; k=`ls *_B02.TIF | sed -e "s/B02\.TIF//g"`; gdalbuildvrt -resolution user -tr 20 20 -separate -overwrite $k\ALLBANDS_20m.vrt *.TIF; python $cmd $k\ALLBANDS_20m.vrt --output $PWD; gdalbuildvrt -separate -overwrite $k\10m.vrt *_B0[2-48].TIF; gdalbuildvrt -separate -overwrite $k\20m.vrt *_B0[5-7].TIF *_B8A.TIF *_B1[0-1].TIF; gdalbuildvrt -separate -overwrite $k\60m.vrt *_B01.TIF *_B09.TIF; gdal_translate -of GTiff -ot Byte -scale 0 3500 -b 3 -b 2 -b 1 -co "COMPRESS=DEFLATE" -co "PREDICTOR=2" -co "TILED=YES" $k\10m.vrt $k\QUICKLOOK.TIF; cd $1; done; done

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

for i in `ls $1`
    do for j in `ls $1/$i`
        do cd $1/$i/$j/$3
        k=`ls *_B02.TIF | sed -e "s/B02\.TIF//g"`
        gdalbuildvrt -resolution user -tr 20 20 -separate -overwrite $k\ALLBANDS_20m.vrt *.TIF
        python $contiguity $k\ALLBANDS_20m.vrt --output $PWD
        gdalbuildvrt -separate -overwrite $k\10m.vrt *_B0[2-48].TIF
        gdalbuildvrt -separate -overwrite $k\20m.vrt *_B0[5-7].TIF *_B8A.TIF *_B1[0-1].TIF
        gdalbuildvrt -separate -overwrite $k\60m.vrt *_B01.TIF *_B09.TIF
        gdal_translate -of GTiff -ot Byte -a_nodata 0 -scale 1 3500 1 255 -b 4 -b 3 -b 2 \
        -co "COMPRESS=JPEG" -co "PHOTOMETRIC=YCBCR" -co "TILED=YES" \
        $k\10m.vrt $k\tmp.TIF
        # python $contrast --filename $k\10m.vrt --out_fname $k\tmp.TIF \
        #   --src_min 1 --src_max 3500 --out_min 1
        gdalwarp -t_srs "EPSG:4326" -tap -tap -co "COMPRESS=JPEG" \
        -co "PHOTOMETRIC=YCBCR" -co "TILED=YES" -tr 0.0001 0.0001 \
        $k\tmp.TIF $k\QUICKLOOK.TIF
        rm $k\tmp.TIF
        gdaladdo -r average $k\QUICKLOOK.TIF 2 4 8 16 32
        cd $1
    done
done

cd $cwd
