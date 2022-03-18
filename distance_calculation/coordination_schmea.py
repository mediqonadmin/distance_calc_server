from sparkql import Struct, Decimal, Integer, Float

from de.mediqon.utils.sparkql_utils.fields import Varchar


class CoordinationRequestSchema(Struct):
    key1 = Varchar(nullable=False, length=200)
    longitude1 = Float(nullable=False)
    latitude1 = Float(nullable=False)
    key2 = Varchar(nullable=False, length=200)
    longitude2 = Float(nullable=False)
    latitude2 = Float(nullable=False)


class CoordinationItemSchema(Struct):
    key = Varchar(nullable=False, length=200)
    longitude = Float(nullable=False)
    latitude = Float(nullable=False)


class DistanceDurationSchema(Struct):
    key1 = Varchar(nullable=False, length=200)
    key2 = Varchar(nullable=False, length=200)
    duration = Float(nullable=False)
    distance = Float(nullable=False)


CoordinationFileDbTypes = {CoordinationRequestSchema.key1.NAME: 'str',
                           CoordinationRequestSchema.key2.NAME: 'str',
                           CoordinationRequestSchema.longitude1.NAME: 'float',
                           CoordinationRequestSchema.latitude1.NAME: 'float',
                           CoordinationRequestSchema.longitude2.NAME: 'float',
                           CoordinationRequestSchema.latitude2.NAME: 'float'}

DistanceDurationDbTypes = {DistanceDurationSchema.key1.NAME: 'str',
                           DistanceDurationSchema.key2.NAME: 'str',
                           DistanceDurationSchema.duration.NAME: 'float',
                           DistanceDurationSchema.distance.NAME: 'float'}
