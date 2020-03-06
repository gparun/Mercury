# ConsumingAPI
We will build up from the simple retriever to the retriever who can persist data, and then we will turn into multithreaded lambda retriever.
We will discover nasty surprises along the way and learn how to to builld in python, how run python in AWS, and how to operate python.

### Which prerequisites do I need?
1. Install the latest Python for your platform: https://www.python.org/downloads/
2. Install Install virtualenv: https://virtualenv.pypa.io/en/latest/installation.html#via-pip
3. Install AWS CLI (recommended): https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html
4. Install AWS SAM CLI: https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-install.html
5. Install PyCharm or use IDE/editor of choice: https://www.jetbrains.com/pycharm/download/ 

## How do I install app?
1. Create a virtual environment using your env of choice and install requirements:
```
virtualenv env
source ./bin/activate
pip install -R requirements.txt
```
2. Contact denis_petelin at epam.com to get IEX token to connect to the datasource.
3. Set the following environment variables:
API_TOKEN=pk_ABCDEF;
TEST_ENVIRONMENT=True;
4. Run ```python handler.py```
