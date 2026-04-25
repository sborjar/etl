# Callevo ETL 

## Tables

| Table | Relationship |
| :--- | :--- | 
| calls | |
| dialer_campaings | calls.camp_id = dialer_campaings.camp_id |
| tenant | dialer_campaings.tenantid = tenant.tenantid |
| users | calls.userfield = users.userid |

### Required fields

| Field | Data |
| :--- | :--- | 
| **DIALER CAMPAIGNS** |
| tenantid | dialer_campaings.tenantid |
| **TENANT** |
| tenantuid | tenant.tenantuid  |
| dialer | tenant.authdomain |
| billingtype | tenant.billingtype |
| bill_rate | tenant.bill_rate |
| increments | tenant.increments |
| **CALLS** |
| camp_id | calls.camp_id |
| calldate | cast( calls.calldate AS DATE ) |
| agents | count( DISTINCT calls.agentid ) |
| totalcalls | count( calls.callid ) | 
| totalagentcalls | sum((CASE WHEN ( calls.callresult = 1 ) THEN 1 ELSE 0 END )) |
| totaldrops | sum((CASE WHEN ( calls.callresult = 5 ) THEN 1 ELSE 0 END )) |
| billsec | sum( calls.billsec * 1.3 ) |
| units | sum(IF((calls.billsec = 0 ),0,greatest(3,ceiling(( calls.billsec / 6 )* 1.3 )))) |

# Strategy

**Objective**  
Retrieve raw data from the database, clean, link, and process it locally, and then upload the summarized results to the database in a results table.


# ETL
## E - Extraction

Count the records for day in the calls table
```sql
SELECT COUNT(callId) FROM calls WHERE DATE(calldate) = "2026-04-23"
```
| DATE | RECORDS | ELAPSED | PLACE |
| --- | ---: | ---: | :---: | 
| 2026-04-23 | 270,125 | 10.881 s | Navicat Local |
| 2026-04-22 | 225,626 |  5.073 s | Navicat Local |
| 2026-04-21 | 338,405 |  5.014 s | Navicat Local |
| 2026-04-20 | 205,070 |  5,570 s | Navicat Local |

Display today's records in the "calls" table, including all fields .

```sql
SELECT 
    callid, calldate, leadid, callresult, agentdisp,
    agentid, origagentid, linkedcall, calltype,
    revenue, dnis,  phone, camp_id, poolid, processed,
    callduration, billsec, holdtime, position, outbchannel,
    cuniqueid, transituniqueid, userfield, amd,
    amdreason, hangupcause,  archived, folder, waiting,
    talked, wrapped, aholdtime, channel, transitchannel,
    calldata, pepperclient, resultid, file_ext,
    recordingidname, datetime, dump, pushresult, exported,
    exported_date, tenantid, trynumber, reccalled,
    sessionid,  queued, answered, hangup, started,
    cmpphone, created_at, updated_at, deleted_at, alternator,
    surveyid, sla, dbsync, amdresult, enterqueue, connect,
    callnode, cinfo, ccall, dispositioned, prefixid, dateofcall
  FROM calls WHERE DATE(calldate) = "2026-04-23"
```
| DATE | RECORDS | ELAPSED | PLACE |
| --- | ---: | ---: | :---: | 
| 2026-04-23 | 277,380 | 20.847 s | Navicat Local |

Display today's records in the "calls" table, including necesary fields.

```sql
SELECT callid, calldate, callresult, agentdisp, agentid, calltype, camp_id, callduration, billsec, hangupcause, waiting, talked, wrapped, pushresult, tenantid, trynumber, reccalled, sla, dispositioned, prefixid FROM calls WHERE DATE(calldate) = "2026-04-23"
```
| DATE | RECORDS | ELAPSED | SAVING CSV | PLACE |
| --- | ---: | ---: | ---: |  :--- | 
| 2026-04-22 | 225,626 | 6.650 s | 0 s | Navicat Local |
| 2026-04-22 | 225,626 | 3.572 s | 1.489 s | 172.30.0.155 |
| 2026-04-23 | 311,519 | 8.960 s | 0 s | Navicat Local |
| 2026-04-23 | 321,798 | 3.313 s | 1.051 s | 172.30.0.155 |
| 2026-04-01 | 132,209 | 5.572 s | 0 s | Navicat Local |
| 2026-04-01 | 132,209 | 3.298 s | 0.655 s | 172.30.0.155 |

## T - Transform

