#!/bin/bash


for ((i=1901; i<2003; i=i+1))
	do	
		#echo https://www.ncei.noaa.gov/pub/data/noaa/$i/
		wget -r -np https://www.ncei.noaa.gov/pub/data/noaa/$i/
	done

