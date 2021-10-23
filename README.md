# Concierge Prototype

This is a functional prorotype of Concierge.  These utilities are intended
to make it easy for a project to generate a narrative about a specific study.

## Design

There are three main components.
- Provider: This is responsible for retrieving information for Study from some
            canonical source.  Currently there is just a Google Sheets Provider.
- Transfer: This handles transferring the data from the source into the KBase
            staging area.  Currently just Globus is supported.
- Concierge: This is handles most of the interactions with KBase.


## Configuration

The configuration is captured in the config.yaml file.  There are several 
provider and transfer specific settings as well as general settings.

### General

* User: The kbase username (this can be cross matched against the token account)
* Provider: Specifies the provider (currently just `Gsheets`)
* Transfer: Specifies the transfer agent (currently just `Globus`)

### Sheets (Google Sheets Provider)

This is for the Google Sheets Provider

* TokenFile: ./secrets/kbase-gcp-7ae5d8a46ffe.json
* Workbook: Workbook name
* Study: Study sheet name (`Study`)
* Samples: Samples sheet name (`Samples`)
* Data: A dictionary of types and their sheets (currently just metagenome is supported)

### Globus (Globus Transfer)

This is for the Globus Transfer Handler.

* SourceEndpoint: Globus Source Endpiont (get from Globus)
* DestinationEndpoint: KBase Endpoint (`c3c0a65f-5827-4834-b6c9-388b0b19953a`)
* ClientId: Globus API token Client Id (generate this via Globus)

## TODO

Document how to set up a Google API token and Globus API refresh token
