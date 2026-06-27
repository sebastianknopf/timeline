# Data Quality Metrics

This document describes the basic concept behind the data quality metrics for Timeline.

For the overall system architecture, see [docs/ARCHITECTURE.md](ARCHITECTURE.md).

## General Purpose

Data quality monitoring is one of the key functionalities of Timeline. In order to monitor data quality, there're two main streams for monitoring: The technical / infrastructure-based monitoring and the consistency / data quality monitoring.

## Terminology

Whereas Timeline normally speaks about trips, stop times, routes and other public transport domain objects, the data quality metrics are mainly based on **entities**. This is because different realtime pipelines may load completely different objects before deriving normalized trips and stop times out of the raw data. For clarification, the following terminology is defined:

- A **trip** is a well-known (normalized) trip object which is expected to have a corresponding nominal trip in the database
- An **entity** is the smallest object contained in the raw realtime pipeline data which must be normalized first to a trip.

## Technical / Infrastructure Monitoring

The technical and infrastructure monitoring reports metrics about **each request per pipeline** including the total number of contained entities, the age of the data and a status code regarding the data extraction. 

- The age is determined by the corresponding `timestamp` fields of the raw data. If not applicable, the current timestamp should be considered by default.
- The status code is a HTTP status code in most times when using HTTP based pipelines. If not applicable (for example in a file-based pipeline), the status code shall be ignored or filled with the corresponding HTTP status code which would suit best.

## Consistency / Data Quality Monitoring

The consistency and data quality monitoring reports quality issues **one time per operation day, entity and pipeline** by adding a flag for the corresponding realtime entity and the issue type which was found during data assessment. Following issue types are currently defined:

| Issue Type | Meaning | Technical ID |
| ---- | ---- | ---- |
| `OperatorIdIsNull` | An operator ID has not been defined or is empty for the particular entity | 1 |
| `RouteIdIsNull` | A route ID has not been defined or is empty for the particular entity | 2 |
| `OperationDayIsNull` | An operation day date is empty or has not been defined for the particular entity | 3 |
| `RouteIdNonGlobal` | The detected route ID does not match the pattern for a global ID  | 4 |
| `StopIdNonGlobal` | The detected stop ID does not match the pattern for a global ID | 5 |
| `TripIdNonGlobal` | The detected trip ID does not match the pattern for a global ID | 6 |
| `TripNotMonitored` | The entity was found in the data but does not contain any realtime information | 7 |
| `TripPredictionInaccurate` | The entity contains realtime information but is flagged as inaccurate | 8 |
| `StartStopIdNull` | The entity does not contain any reference to start stop ID | 9 |
| `DestinationStopIdNull` | The entity does not contain any reference to the end stop ID | 10 |
| `NotCompleteStopSequence` | The contained stops in the entity do not represent the complete stop sequence | 11 |
| `NoNominalTripFound` | No corresponding nominal entity was found for this entity | 12 |
| `NoAmbiguousNominalTripFound` | Multiple possible nominal entity candidates were found for this entity | 13 |
| `AimedDepartureTimeBeforeArrivalTime` | The aimed departure timestamp of a stop time lays before the aimed arrival timestamp | 14 |
| `EstimatedDepatureTimeBeforeArrivalTime` | The estimated departure timestamp of a stop time lays before the estimated arrival timestamp | 15 |
| `UnexpectedStopFound` | An unexpected stop was found in the realtime data which is not part of the nominal trip | 16 |
| `ExpectedStopMissing` | The expected stop was not contained in the realtime data **even if the realtime stop sequence was stated as complete stop sequence explicitly** | 17 |

Please note, that the exact issues monitored and reported strongly depend on the realtime pipeline type, as not all data contain all information by definition. For example the `gtfsrt-tripupdates` realtime pipeline will not monitor the issue type `TripNotMonitored` or `TripPredictionInaccurate` as this information is simply not contained by the GTFS-RT data.

Also be aware of the interpretation of the issue types. The interpretation is only possible in context of a pipeline type seamlessly.

## Internal Architecture

### QualityReportService
The `QualityReportService` offers all methods needed for logging the request basic measures and the quality issues. The service is meant to be used **on pipeline-run level** meaning that each instance is used in one pipeline run. Additionally, the service can be used by the loading service in order to report quality issues which are detected at loading / matching level at all.

It holds exactly one `RequestRecord` object (which is meant to cover the basic requests metrics) and many `QualityIssueRecords` for all quality issues detected during a pipeline run.

### Implementation on Pipeline Level

Each pipeline, either nominal or realtime should inherit from the corresponding base class `NominalPipeline` and `RealtimePipeline`. Those abstract base classes offer methods to support data quality reporting in an object-oriented way. 

Please note that each pipeline implementation needs to call `super().__init__()` in her constructor in order to initialize the super class correctly with the `QualityReportService`. The structure for a pipeline's `execute` method should look as follows:

```python
async def execute(...) -> None:

    # do some basic setup stuff here

    try:

        # to the pipeline stuff here and catch exceptions

        # report each quality issue ON EVERY OCCURENCE
        # if the same quality issues was already reported for the particular entity
        # the unitifaction is done in the QualiyReportService
        self.report_quality_issue(
            instance=instance,
            pipeline=pipeline,
            timestamp=now_processor_tz,
            entity_id=entity.id,
            issue_type_id=QualityIssue.RouteIdNonGlobal,
            assessment_value=route_id
        )

        # load the realtime data into database and catch the result
        # quality issues occured during loading must be handled here!
        result: RealtimeLoadingResult = await self._loading_service.load_realtime_trip_and_stop_times(
            instance_id=instance.id,
            trip=mapped_trip,
            stop_times=mapped_stop_times,
        )

        if realtime_loading_result == RealtimeLoadingResult.SUCCESS_DIRECT:
            loaded_direct_trip_count += 1
        elif realtime_loading_result == RealtimeLoadingResult.SUCCESS_MATCHED:
            loaded_matched_trip_count += 1
        elif realtime_loading_result == RealtimeLoadingResult.NO_NOMINAL_TRIP_FOUND:
            self.report_quality_issue(
                instance=instance,
                pipeline=pipeline,
                timestamp=now_processor_tz,
                entity_id=entity.id,
                issue_type_id=QualityIssue.NoNominalTripFound
            )
        elif realtime_loading_result == RealtimeLoadingResult.NO_AMBIGUOUS_NOMINAL_TRIP_FOUND:
            self.report_quality_issue(
                instance=instance,
                pipeline=pipeline,
                timestamp=now_processor_tz,
                entity_id=entity.id,
                issue_type_id=QualityIssue.NoAmbiguousNominalTripFound
            )

        # report the request result EXACTLY ONE TIME
        self.report_request(
            instance=instance,
            pipeline=pipeline,
            timestamp=now_processor_tz,
            num_entities=trip_update_count,
            loaded_direct_trip_count=loaded_direct_trip_count,
            loaded_matched_trip_count=loaded_matched_trip_count,
            age_seconds=(now_processor_tz - feed_timestamp_utc).total_seconds() if feed_timestamp_utc else 0,
            status_code=200
        )

    finally:

        # submit all collected quality reports here
        await self.submit_quality_report(instance=instance)
```

Notes:

- the module `src/processor/common/quality_issues.py` contains an enum with the readable issue codes and their internal IDs.
- the `QualityReportService` instance **MUST BE** intialized on pipeline level, **NOT** in `__main__.py` as the service needs to be owned by the pipeline itself, not globally. Otherwise, the pipelines would infer each other in negative way.