CREATE TABLE [dbo].[DimPokemonBerry] (

	[Id] int NULL, 
	[ExtractDate] date NULL, 
	[Name] varchar(8000) NULL, 
	[GrowthTime] int NULL, 
	[MaxHarvest] int NULL, 
	[NaturalGiftPower] int NULL, 
	[Size] int NULL, 
	[Smoothness] int NULL, 
	[SoilDyrness] int NULL, 
	[FirmnessName] varchar(8000) NULL, 
	[FirmnessUrl] varchar(8000) NULL, 
	[ItemName] varchar(8000) NULL, 
	[ItemUrl] varchar(8000) NULL, 
	[NaturalGiftTypeName] varchar(8000) NULL, 
	[NaturalGiftTypeUrl] varchar(8000) NULL, 
	[FlavorPotency] int NULL, 
	[FlavorName] varchar(8000) NULL, 
	[FlavorUrl] varchar(8000) NULL, 
	[Partition] varchar(8000) NULL, 
	[RowHash] varchar(8000) NULL, 
	[ValidFrom] date NULL, 
	[ValidTo] date NULL, 
	[IsCurrent] bit NULL, 
	[SurrogateKey] int NULL
);