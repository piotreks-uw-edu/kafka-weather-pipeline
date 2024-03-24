CREATE TABLE HighPollution (
	Country VARCHAR(64),
	PM10 Float
)
GO

CREATE TABLE Correlation (
	WeatherFactor VARCHAR(64),
	PollutionFactor VARCHAR(64),
	Pearson Float
)
GO

CREATE TABLE CapitalPollution (
	Capital VARCHAR(64),
	NO2 Float
)
GO

CREATE TABLE Distribution (
	Lat FLoat,
	Lon Float,
	PM10 FLoat
)