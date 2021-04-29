echo -n "Nombre de duplications du fichier ?"
read number


cat lorem_original.txt > fat_lorem.txt
sleep 0.1
for ((i=1;i<=$number;i++));
do
	echo -n "Duplication..."
	cat fat_lorem.txt fat_lorem.txt > fat_lorem2.txt
	sleep 0.1
	mv fat_lorem2.txt fat_lorem.txt
done
