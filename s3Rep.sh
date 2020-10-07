#!/bin/bash

dbDirectory="/history/s3Rep"
triageDirectory="/triage"
credentialsFile="s3Rep.json"
declare -a sourceDirectories=$(echo -n '(';jq '.s3_replication|.[]|.source_directory_path' $dbDirectory'/'$credentialsFile;echo -n ')')

for sourceDirectory in "${sourceDirectories[@]}"
do
	qq fs_delete_tree --path "$triageDirectory""$sourceDirectory" --force
	
	bucketName=$(jq -r '.s3_replication|.[]|select(.source_directory_path=="'"$sourceDirectory"'")|.bucket' $dbDirectory'/'$credentialsFile)
	objectStoreAddress=$(jq -r '.s3_replication|.[]|select(.source_directory_path=="'"$sourceDirectory"'")|.object_store_address' $dbDirectory'/'$credentialsFile)
	regionName=$(jq -r '.s3_replication|.[]|select(.source_directory_path=="'"$sourceDirectory"'")|.region' $dbDirectory'/'$credentialsFile)
	objectFolderName=$(jq -r '.s3_replication|.[]|select(.source_directory_path=="'"$sourceDirectory"'")|.object_folder' $dbDirectory'/'$credentialsFile)
	accessKeyID=$(jq -r '.s3_replication|.[]|select(.source_directory_path=="'"$sourceDirectory"'")|.access_key_id' $dbDirectory'/'$credentialsFile)
	secretAccessKey=$(jq -r '.s3_replication|.[]|select(.source_directory_path=="'"$sourceDirectory"'")|.secret_access_key' $dbDirectory'/'$credentialsFile)
		
	policyName=$(jq -r '.s3_replication|.[]|select(.source_directory_path=="'"$sourceDirectory"'")|.snapshot_name' $dbDirectory'/'$credentialsFile)
	echo $policyName
	qq snapshot_create_snapshot --name $policyName -t "7days" --path "$sourceDirectory"
	snapshots=($(qq snapshot_list_snapshots --all| jq '.entries|.[]|select(.name=="'$policyName'")|.id'|tail -n 2))
	
	declare -a createdFiles=$(echo -n '(';qq snapshot_diff --newer-snapshot ${snapshots[1]} --older-snapshot ${snapshots[0]}|jq '.entries|.[]|select (.op=="CREATE")|.path';echo -n ')')
	for createdFile in "${createdFiles[@]}"
	do
		fileType=$(qq fs_file_get_attr --path "$createdFile"|jq -r '.type')
		
		if [[ $fileType == "FS_FILE_TYPE_DIRECTORY" ]]
		then
			declare -a filesInNewDirectory=$(echo -n '(';qq fs_walk_tree --path "$createdFile" --file-only |jq '.tree_nodes|.[]|.path';echo -n ')')
				
				for fileInNewDirectory in "${filesInNewDirectory[@]}"
				do
					newMainDirectory="$triageDirectory"
					declare -a newDirectoryPath=$(echo -n '(';echo -n  "$fileInNewDirectory"|awk 'BEGIN{res=""; FS="/";}{ for(i=2;i<=NF-1;i++) {print "\""$i"\""}}';echo -n ')')
						
					for newDirectory in "${newDirectoryPath[@]}"
					do
						checkPath="$newMainDirectory"/"$newDirectory"
						fileID=$(qq fs_read_dir --path "$checkPath"|jq -r '.id')
						if ! [[ $fileID =~ ^[0-9]+$ ]] 
						then
							qq fs_create_dir --path "$newMainDirectory" --name "$newDirectory"
						fi
						newMainDirectory="$newMainDirectory"/"$newDirectory"
					done
						
						qq fs_copy "$fileInNewDirectory" "$triageDirectory""$fileInNewDirectory"
				done
		elif [[ $fileType == "FS_FILE_TYPE_FILE" ]]
		then
					
			newMainDirectory="$triageDirectory"				
			declare -a newDirectoryPath=$(echo -n '(';echo -n  "$createdFile"|awk 'BEGIN{res=""; FS="/";}{ for(i=2;i<=NF-1;i++) {print "\""$i"\""}}';echo -n ')')
						
			for newDirectory in "${newDirectoryPath[@]}"
			do
				checkPath="$newMainDirectory"/"$newDirectory"
				fileID=$(qq fs_read_dir --path "$checkPath"|jq -r '.id')
							
				if ! [[ $fileID =~ ^[0-9]+$ ]]
				then
					qq fs_create_dir --path "$newMainDirectory" --name "$newDirectory"
				fi
					newMainDirectory="$newMainDirectory"/"$newDirectory"
			done
			qq fs_copy "$createdFile" "$triageDirectory"/"$createdFile"
		fi
	done
	declare -a createdFiles=$(echo -n '(';qq snapshot_diff --newer-snapshot ${snapshots[1]} --older-snapshot ${snapshots[0]}|jq '.entries|.[]|select (.op=="MODIFY")|.path';echo -n ')')
        for createdFile in "${createdFiles[@]}"
        do
		fileType=$(qq fs_file_get_attr --path "$createdFile"|jq -r '.type')
		if [[ $fileType == "FS_FILE_TYPE_FILE" ]]
                then
			newMainDirectory="$triageDirectory"
			declare -a newDirectoryPath=$(echo -n '(';echo -n  "$createdFile"|awk 'BEGIN{res=""; FS="/";}{ for(i=2;i<=NF-1;i++) {print "\""$i"\""}}';echo -n ')')
			for newDirectory in "${newDirectoryPath[@]}"
                	do
                		checkPath="$newMainDirectory"/"$newDirectory"
				fileID=$(qq fs_read_dir --path "$checkPath"|jq -r '.id')

                        	if ! [[ $fileID =~ ^[0-9]+$ ]]
                        	then
                      			qq fs_create_dir --path "$newMainDirectory" --name "$newDirectory"
                        	fi
                        	newMainDirectory="$newMainDirectory"/"$newDirectory"
             		done
               	qq fs_copy "$createdFile" "$triageDirectory"/"$createdFile"
                fi
        done
	declare -a triageSourceDir=$(echo -n '(';echo "$triageDirectory""$sourceDirectory";echo -n')')
	triageSourceDirID=$(qq fs_read_dir --path "$triageSourceDir"|jq -r '.id')
	qq replication_create_object_relationship --source-directory-id "$triageSourceDirID" --object-store-address "$objectStoreAddress" --object-folder "$objectFolderName" --bucket "$bucketName" --region "$regionName" --access-key-id "$accessKeyID" --secret-access-key "$secretAccessKey"
done
