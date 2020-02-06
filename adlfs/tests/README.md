# Testing

adlfs uses the [Azurite][azurite] emulator for testing.
This is implemented using a pytest fixture and the python Docker SDK.  The docker image will be pulled and started, then torn
down automatically using pytest.

Alternatively, you can start a docker container running Azurite with the following:

    docker run -p 10000:10000 mcr.microsoft.com/azure-storage/azurite azurite-blob --blobHost 0.0.0.0 --debug /tmp/debug.log 


[azurite]: https://github.com/Azure/Azurite