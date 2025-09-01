cf set-env indb-embedding DB_ADDRESS 291933b3-a304-499f-8649-14ec7ce3ce9a.hana.prod-ap10.hanacloud.ondemand.com
cf set-env indb-embedding DB_PORT 443
cf set-env indb-embedding DB_USER DBADMIN
cf set-env indb-embedding DB_PASSWORD Testing123
cf set-env indb-embedding AICORE_AUTH_URL 'https://dial-3-0-zme762l7.authentication.ap10.hana.ondemand.com/oauth/token'
cf set-env indb-embedding AICORE_CLIENT_ID 'sb-92376755-5757-46d2-a7f6-088e4a74c92f!b54420|aicore!b1456'
cf set-env indb-embedding AICORE_CLIENT_SECRET '0aa7f2f5-83e1-4149-b7bd-7ffc722242d0$KRekCt3pC04wa90Od2Ba5YYrDw-BPGLhSGGJr1rZ4qo='
cf set-env indb-embedding AICORE_BASE_URL 'https://api.ai.prod.ap-southeast-2.aws.ml.hana.ondemand.com/v2'
cf set-env indb-embedding AICORE_RESOURCE_GROUP 'default'
cf restage indb-embedding

cf set-env kgapp DB_ADDRESS 291933b3-a304-499f-8649-14ec7ce3ce9a.hana.prod-ap10.hanacloud.ondemand.com
cf set-env kgapp DB_PORT 443
cf set-env kgapp DB_USER DBUSER
cf set-env kgapp DB_PASSWORD Testing123
cf set-env kgapp AICORE_AUTH_URL 'https://dial-3-0-zme762l7.authentication.ap10.hana.ondemand.com/oauth/token'
cf set-env kgapp AICORE_CLIENT_ID 'sb-92376755-5757-46d2-a7f6-088e4a74c92f!b54420|aicore!b1456'
cf set-env kgapp AICORE_CLIENT_SECRET '0aa7f2f5-83e1-4149-b7bd-7ffc722242d0$KRekCt3pC04wa90Od2Ba5YYrDw-BPGLhSGGJr1rZ4qo='
cf set-env kgapp AICORE_BASE_URL 'https://api.ai.prod.ap-southeast-2.aws.ml.hana.ondemand.com/v2'
cf set-env kgapp AICORE_RESOURCE_GROUP 'default'
cf restage kgapp

cf set-env ui5node-poc-knowledgegraph KG_ENDPOINT https://kgapp.cfapps.ap10.hana.ondemand.com
cf set-env ui5node-poc-knowledgegraph PY_ENDPOINT https://indb-embedding.cfapps.ap10.hana.ondemand.com