#!/bin/sh

DATE_UTC=$(date -u)
curl -X PUT -T $TRAVIS_BUILD_DIR/target/$BLOB_NAME_URL -H "x-ms-date: $DATE_UTC" -H "x-ms-blob-type: BlockBlob" https://$AZURE_STORAGE_ACCOUNT.blob.core.windows.net/$BLOB_CONTAINER/$BLOB_NAME_URL$SAS_TOKEN
