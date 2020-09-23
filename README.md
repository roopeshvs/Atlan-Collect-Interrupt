# Atlan Collect Interrupt

Enables Pause, Resume, and Terminate operations on Long Running Tasks with the use of API Calls.

## Data Used:

https://www.kaggle.com/imoore/60k-stack-overflow-questions-with-quality-rate

## How to install and run?

Create a Python Virtual Environment and Activate it.

Clone this repository using the command,

`git clone https://github.com/roopeshvs/Atlan-Collect-Interrupt`

Go to the Root of the Project and Install Requirements

`cd Atlan-Collect-Interrupt`

`pip install -r requirements.txt`

You are all set. Run the application using the command,

`python app.py` or `python3 app.py`

## How to use?

Refer to the API Documentation here,

https://documenter.getpostman.com/view/6700377/TVKD3y9t

First, populate the DB by uploading the CSV file downloaded from the dataset link above.

Pause, Resume, and Stop the Task by using the Task ID visible in the terminal instance and appropriate API request.

Notice that the transaction is atomic and stopping midway does not cause partial write.

You can then export and do all the operations over.

## To-Do

- [ ] Integrate With Front-End
- [ ] Add Linting and Follow PEP8 Guidelines
- [ ] Allow Easily Customisable Tables and Operations
