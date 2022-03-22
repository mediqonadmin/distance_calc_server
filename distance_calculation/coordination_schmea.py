
class CoordinationRequestSchema:
    key1 = "key1"
    longitude1 = "longitude1"
    latitude1 = "latitude1"
    key2 = "key2"
    longitude2 = "longitude2"
    latitude2 = "latitude2"


class CoordinationItemSchema:
    key = "key"
    longitude = "longitude"
    latitude = "latitude"


class DistanceDurationSchema:
    key1 = "key1"
    key2 = "key2"
    duration = "duration"
    distance = "distance"


CoordinationFileDbTypes = {CoordinationRequestSchema.key1: 'str',
                           CoordinationRequestSchema.key2: 'str',
                           CoordinationRequestSchema.longitude1: 'float',
                           CoordinationRequestSchema.latitude1: 'float',
                           CoordinationRequestSchema.longitude2: 'float',
                           CoordinationRequestSchema.latitude2: 'float'}

DistanceDurationDbTypes = {DistanceDurationSchema.key1: 'str',
                           DistanceDurationSchema.key2: 'str',
                           DistanceDurationSchema.duration: 'float',
                           DistanceDurationSchema.distance: 'float'}


class DistanceCoordinationFileHelper:
    coordination_file_prefix: str = "coordination_items_"
    result_file_prefix = "results_items_"
