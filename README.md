# indicative-data-model
A custom module for constructing Snowplow event data to be ready and in an appropriate shape for [Indicative](https://www.indicative.com/), so it can read your event data from your data warehouse. This runs off the raw atomic `events` table in your Snowplow warehouse, and this module is designed to be added to the `sql-runner` orchestration software, used internally by Snowplow. 

The model is designed to be flexible enough include any custom events from your tracking set up, and include data from your custom events and entities you may be tracking. 

The output is an event level table ready for consumption by Indicative. This module is currently for Redshift only. 

## Module Structure
The module is basically a simple `SELECT` statement that in the first CTE (`base_table`) selects all the main columns from the `events` table.

The following CTE (`child_table_joins`) then joins to all the custom entity and event tables (in Redshift) and selects relevant columns.

The next CTE (`user_mapping`) creates a user mapping table so that the aliases user ID can be applied to anonymous events, by looking through the users' history and if there is a custom user ID set for this user, uses this. This is pulled from a `user_stitching` table that is incrementally updated with new user data each run.  

The files contained in this repo are all of files and folders necessary for integrating into `sql-runner`.

## Integrating into sql-runner
You will need to be running at least [`v1.0.0`](https://github.com/snowplow/data-models) of the Snowplow data model in order to integrate this custom module.

To integrate this custom module into your `sql-runner` model, you can clone this repo, and move the files into the `custom` folder structure within your existing model - both in the appropriate `playbooks` directory and the `sql` directory.

You will also need to edit a couple of sections of the main query found in [03-indicative-export-staged.sql](web/v1/redshift/sql-runner/sql/custom/01-indicative-export/03-indicative-export-staged.sql).

## Editing the main query
### `custom_event_name`
The first area that you may customise is the `custom_event_name` field. 

```
CASE
  WHEN event_name = 'page_view' AND page_urlpath = '/' THEN 'homepage_view'
  ELSE event_name
END AS custom_event_name, -- example derived event name field
```
In the above example, we use a `CASE` statement to pick out page views of the homepage as a custom event name (`homepage_view`) that will populate this column. In Indicative, you may want to call out something like this as a specific event, rather than filtering for `page_views` and then filtering for page URL being the homepage (`/`). This is less performant within Indicative, so calling out custom event names that are derived from your Snowplow events can be useful.

### Child Table Joins
The next area that must be customised for your use case is the `child_table_joins` CTE. The query within this repo is an example - for your use case, you should join to the necessary child tables for both standard events and entities (such as link clicks and the web page context) and your custom events and entities. You should also select all relevant columns from these tables. These extra columns you add to your model will need adding to the [`01-indicative-export-setup.sql`](web/v1/redshift/sql-runner/sql/custom/01-indicative-export/01-indicative-export-setup.sql) file.
