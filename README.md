# Overview

### The Client's Question (Business problem)
How much revenue is the client getting from external Search Engines, such as Google, Yahoo and
MSN, and which keywords are performing the best based on revenue?

### Development Requirements
    1. Create a Python application that needs to be deployed and executed within AWS
    2. The Python application needs to contain at least one class
    3. The Python application needs to accept a single argument, which is the file that needs to be
processed.

Bonus Points: Unit test cases, serverless deployment scripts, Business problem presentation


# Note Taking on Assesment

### Thoughts on Processing
Given inital take home requirements, size of file, and then need to produce something within shortened time limits here file processing can take place very easily within a lambda function within AWS. Processing file can hold checks for data quality retrived from `space_bloom` client, ability to process and read in entire file given file size, and then finally depoist the file into an output bucket within s3. That output bucket can be created with read_access granted to our `space_bloom` client. This should satisy the below stated requirements mentioned within the take-home.

#### Output File Requirements

##### Format
- Tab-delimited file
- Named: `YYYY-mm-dd_SearchKeywordPerformance.tab` where the date reflects the date the application executed

##### Columns
The file must include a header row using the column names below, in this order:

| Column | Example |
|---|---|
| Search Engine Domain | google.com |
| Search Keyword | Laffy Taffy |
| Revenue | 12.95 |

##### Sorting
- Rows sorted by **Revenue**, descending


#### Stretch Goal
It should be noted that within the takehome production level requirements are quite different than what has been provided to the internal team here within Adobe. File size has grown exponentially in producion with potential expectations of the file being at or greater than 10TB. Our current setup using Lambda Functions will in no way shape or form work with the current setup. Timing wise lambda functions have a built in celing of fifteen minutes to execute, and the manner in which we are executing the file is to load it fully into memory which we could not do with a 10GB file. The current ephemeral storage limit for a lambda fucntion is 10GB which we would most likely blow past here.

Given these constraints it is recommend that a slight rewrite of the code occurs to read in the file line by line, writing outptus in chunks here to a folder within our output bucket, and then from there an additional service to provide the final summary file as requested above. For ease of use with my downstream I would output a Parquet file that could easily be read into AWS Athena, reordered as necessary, and then depositied into the fina output bucket as shown above.

The solution here is fully serverless and kicked off by the arrival of a file into our S3 Bucket as determined by the `space_bloom` client:

  1. S3 Bucket ( File Landing)
    * Deposit raw tab delimited file into our receiving bucket.
    * Access to bucket is stritcly granted to enable file processing security. Read to client and internal adobe team, and then write granted only to the service which would be delivering the file to our processing pipeline.

  2. Eventbridge ( Trigger )
    * Rule to listen for file creation on the above s3 bucket

  3. ECS Fargate ( Processing )
    * FarGate task will be initiated taking in the arguemnt of the s3bucket location of the latest arrived file in order to take in the file
    * Processing of file will be setup to be processed in batches rather than line by line in order to handle the large amount of rows to be processed given the incresaed file size.
    * Error handling will be present within the processing of batches in order to ensure that any failed batch will be sent to another s3 bucket for future review/ reprocessing at a later point in time.

  4. S3 Bucket ( Output Landing )
    * Files will be stored in raw_output per day per batch of file outputted by the processing container.
    * Files will be stored here in parquet format to make it easier for files to be read into Athena
    * Bucket access here will be limited to scope of 

  5. AWS Atherna & Event Bridge  & Lambda ( Query and Sort )
    * Eventbridge waits to signal Lambda upon exit code zero from Fargate Process
    * Lambda setups up and executes AWS Athena Query
    * Output to S3 Bucket here of the same above with final_output by day 

  6. S3 Bucket ( Sorted output landing)
    * S3 bucket with another group here to handle file landing from Athena exectuion
    * Access set to read only for internal clinet, external client, and then write only to our internal process


  7. Alerting ( SNS )
    * Wrap processing of files, and then also processing of final file output into SNS then whatever final location might need to exist.
    * SNS according to documentation easily works with:
        * email
        * webhook
        * slack
        * opsgenie


### Data
Sorted information is hit level data of website traffic to our external client space_bloom. Data represents what can be termed as a single "hit" or site visit from a party. Client has already implemented event level tracking on their website enabling site visit traffic to be fed into Adobe Analytics 


# `space_bloom` — Hit Data Reference

## `products_list`

A string encoding one or more products, where:

- Products are separated by **commas** (`,`)
- Attributes within each product are separated by **semicolons** (`;`)

### Format

```
[Category];[Product Name];[Number of Items];[Total Revenue];[Custom Event]|[Custom Event];[Merchandising eVar]
```

### Example

```
Computers;HP Pavillion;1;1000;200|201,Office Supplies;Red Folders;4;4.00;205|206|207
```

---

## Attributes

| Attribute | Description |
|---|---|
| `Category` | The product category (e.g. `Shoes`, `Clothes`) |
| `Product Name` | Product identifier — either a product ID or human-readable name |
| `Number of Items` | Quantity of the product |
| `Total Revenue` | Product price. Only actualized when the purchase event is set in `events_list` |
| `Custom Event` | One or more events scoped to this specific product, pipe-delimited (`\|`) |
| `Merchandising eVar` | One or more eVars scoped to this specific product, pipe-delimited (`\|`) |

---

## Notes

- **Multiple custom events** are separated by pipes: `200|201`
- **Multiple merchandising eVars** are also pipe-delimited
- Revenue is **not recorded** unless the purchase event is present in `events_list`


### Questions and Concenrs to Address

1. Expected delivery cadence of file to our processing platform.
2. Varying size of data for processing of file. Would be great to know expected file shape and size by traffic.
3. Test file generation. Does the client mantain a testing, development, or QA site which we could also engage with in order to move in lock step with our own development. In a sense we have a development branch, development aws project, and with these the ability to test before merging into our production project.
4. Work with client to determine if file delivery can be guranteed to have the following:
    4a. Removal of carriage characters within file delivery process termed as `^M$` or `\r\n`
    4b. Determine if file delivery will come with, or without headers
5. Expectation of data retreival. Should it be sent, accessible, or potentially built out with visuals.