{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Control.Monad (when)
import Data.Binary (Binary (get), Word16, Word32, Word64, Word8)
import Data.Binary.Get qualified as BG
import Data.Bits (Bits (shiftL, (.&.), (.|.)))
import Data.ByteString qualified as BS
import Data.ByteString.Lazy qualified as BL
import Data.Graph (Table)
import Data.Int (Int64)
import Data.List (elemIndex, findIndex, findIndices, intercalate)
import Data.Maybe (mapMaybe)
import Data.Text.Lazy (Text)
import Data.Text.Lazy qualified as T
import Data.Text.Lazy.Encoding (decodeLatin1)
import Data.Text.Lazy.IO qualified as IO
import Debug.Trace (trace, traceShow)
import Language.SQL.SimpleSQL.Dialect (Dialect (diAutoincrement), ansi2011)
import Language.SQL.SimpleSQL.Parse qualified as Sql
import Language.SQL.SimpleSQL.Syntax (QueryExpr (..))
import Language.SQL.SimpleSQL.Syntax qualified as Sql
import Numeric (showHex)
import System.Environment (getArgs)
import System.IO.MMap (mmapFileByteStringLazy)

traceS :: (Show a) => String -> a -> a
traceS prefix x = trace (prefix <> ": " <> show x) x

data DatabaseHeader = DatabaseHeader
  { _dbPageSize :: Word16
  , _dbFileFormatWriteVersion :: Word8
  , _dbFileFormatReadVersion :: Word8
  , _dbReservedBytes :: Word8
  , _dbMaxEmbeddedPayloadFraction :: Word8
  , _dbMinEmbeddedPayloadFraction :: Word8
  , _dbLeafPayloadFraction :: Word8
  , _dbFileChangeCounter :: Word32
  , _dbDatabaseSize :: Word32
  , _dbFirstFreelistTrunkPage :: Word32
  , _dbTotalFreelistPages :: Word32
  , _dbSchemaCookie :: Word32
  , _dbSchemaFormatNumber :: Word32
  , _dbDefaultPageCacheSize :: Word32
  , _dbLargestRootBTreePageNumber :: Word32
  , _dbTextEncoding :: Word32
  , _dbUserVersion :: Word32
  , _dbIncrementalVacuumMode :: Word32
  , _dbApplicationId :: Word32
  , _dbVersionValidFor :: Word32
  , _dbSqliteVersionNumber :: Word32
  }
  deriving (Show)

databaseHeaderParser :: BG.Get DatabaseHeader
databaseHeaderParser = do
  BG.skip 16
  pageSize <- BG.getWord16be
  fileFormatWriteVersion <- BG.getWord8
  fileFormatReadVersion <- BG.getWord8
  reservedBytes <- BG.getWord8
  maxEmbeddedPayloadFraction <- BG.getWord8
  minEmbeddedPayloadFraction <- BG.getWord8
  leafPayloadFraction <- BG.getWord8
  fileChangeCounter <- BG.getWord32be
  databaseSize <- BG.getWord32be
  firstFreelistTrunkPage <- BG.getWord32be
  totalFreelistPages <- BG.getWord32be
  schemaCookie <- BG.getWord32be
  schemaFormatNumber <- BG.getWord32be
  defaultPageCacheSize <- BG.getWord32be
  largestRootBTreePageNumber <- BG.getWord32be
  textEncoding <- BG.getWord32be
  userVersion <- BG.getWord32be
  incrementalVacuumMode <- BG.getWord32be
  applicationId <- BG.getWord32be
  BG.skip 20
  versionValidFor <- BG.getWord32be
  sqliteVersionNumber <- BG.getWord32be
  pure $
    DatabaseHeader
      { _dbPageSize = pageSize -- 1 is for 64KB
      , _dbFileFormatWriteVersion = fileFormatWriteVersion
      , _dbFileFormatReadVersion = fileFormatReadVersion
      , _dbReservedBytes = reservedBytes
      , _dbMaxEmbeddedPayloadFraction = maxEmbeddedPayloadFraction
      , _dbMinEmbeddedPayloadFraction = minEmbeddedPayloadFraction
      , _dbLeafPayloadFraction = leafPayloadFraction
      , _dbFileChangeCounter = fileChangeCounter
      , _dbDatabaseSize = databaseSize
      , _dbFirstFreelistTrunkPage = firstFreelistTrunkPage
      , _dbTotalFreelistPages = totalFreelistPages
      , _dbSchemaCookie = schemaCookie
      , _dbSchemaFormatNumber = schemaFormatNumber
      , _dbDefaultPageCacheSize = defaultPageCacheSize
      , _dbLargestRootBTreePageNumber = largestRootBTreePageNumber
      , _dbTextEncoding = textEncoding
      , _dbUserVersion = userVersion
      , _dbIncrementalVacuumMode = incrementalVacuumMode
      , _dbApplicationId = applicationId
      , _dbVersionValidFor = versionValidFor
      , _dbSqliteVersionNumber = sqliteVersionNumber
      }

data PageType = IndexInteriorPage | IndexLeafPage | TableInteriorPage | TableLeafPage
  deriving (Show, Eq)

data PageHeader = PageHeader
  { _pageType :: PageType
  , _firstFreeblockOffset :: Word16
  , _numberOfCells :: Word16
  , _cellContentAreaStartOffset :: Word16
  , _fragmentedFreeBytes :: Word8
  , _rightmostPointer :: Maybe Word32
  }
  deriving (Show)

pageTypeParser :: BG.Get PageType
pageTypeParser = do
  pageType <- BG.getWord8
  case pageType of
    0x02 -> pure IndexInteriorPage
    0x05 -> pure TableInteriorPage
    0x0A -> pure IndexLeafPage
    0x0D -> pure TableLeafPage
    _ -> fail "Unknown page type"

isLeafPage :: PageType -> Bool
isLeafPage IndexLeafPage = True
isLeafPage TableLeafPage = True
isLeafPage _ = False

pageHeaderParser :: BG.Get (PageHeader, Word16)
pageHeaderParser = do
  pageType <- pageTypeParser
  firstFreeblockOffset <- BG.getWord16be
  numberOfCells <- BG.getWord16be
  cellContentAreaStartOffset <- BG.getWord16be
  fragmentedFreeBytes <- BG.getWord8
  rightmostPointer <-
    if isLeafPage pageType
      then pure Nothing
      else Just <$> BG.getWord32be
  pure
    ( PageHeader
        { _pageType = pageType
        , _firstFreeblockOffset = firstFreeblockOffset
        , _numberOfCells = numberOfCells
        , _cellContentAreaStartOffset = cellContentAreaStartOffset
        , _fragmentedFreeBytes = fragmentedFreeBytes
        , _rightmostPointer = rightmostPointer
        }
    , if isLeafPage pageType then 8 else 12
    )

type PageCellPayload = BL.ByteString

type RowId = Int64

data PageCell
  = TableLeafCell
      { _rowId :: RowId
      , _recordValues :: TableRecord
      }
  | TableInteriorCell
      { _leftChildPageNumber :: Word32
      , _rowId :: RowId
      }
  | IndexLeafCell TableRecord
  | IndexInteriorCell
      { _leftChildPageNumber :: Word32
      , _payload :: PageCellPayload
      }
  deriving (Show)

type CellPointer = Word16

{- |
>>> :set -XBinaryLiterals
>>> BG.runGet variantParserWithSize $ BL.pack [0b10000001, 0b00101100]
(172,2)

>>> BG.runGet variantParserWithSize $ BL.pack [0x81, 0x80, 0x00]
(16384,3)
-}
variantParserWithSize :: BG.Get (Word64, Word8)
variantParserWithSize = do
  go 0 0
 where
  hasMoreBytes :: Word8 -> Bool
  hasMoreBytes w = w .&. 0x80 /= 0

  go :: Word8 -> Word64 -> BG.Get (Word64, Word8)
  go i acc = do
    byte <- BG.getWord8
    if i < 8
      then
        let acc' = (acc `shiftL` 7) .|. fromIntegral (byte .&. 0x7F)
         in if hasMoreBytes byte
              then go (i + 1) acc'
              else pure (acc', i + 1)
      else
        -- last byte, the 9th byte
        pure ((acc `shiftL` 8) .|. fromIntegral byte, 9)

variantParser :: BG.Get Word64
variantParser = fst <$> variantParserWithSize

signedVariantParser :: BG.Get Int64
signedVariantParser = fromIntegral . fst <$> variantParserWithSize

pageCellParser :: PageType -> BG.Get PageCell
pageCellParser pageType = do
  case pageType of
    TableLeafPage -> do
      payloadSize <- variantParser
      rowId <- signedVariantParser
      TableLeafCell rowId <$> recordParser
    TableInteriorPage -> do
      leftChildPageNumber <- BG.getWord32be
      TableInteriorCell leftChildPageNumber <$> signedVariantParser
    IndexLeafPage -> do
      payloadSize <- variantParser
      IndexLeafCell <$> recordParser
    IndexInteriorPage -> do
      leftChildPageNumber <- BG.getWord32be
      payloadSize <- variantParser
      payload <- BG.getLazyByteString $ fromIntegral payloadSize
      pure $ IndexInteriorCell leftChildPageNumber payload

pageCellsParser :: Word16 -> PageHeader -> BG.Get [PageCell]
pageCellsParser offset (PageHeader{_pageType = pageType, _numberOfCells = n, _cellContentAreaStartOffset = contentStartOffset}) = do
  cellPointers <- mapM (const BG.getWord16be) [0 .. n - 1]
  BG.skip $ fromIntegral $ contentStartOffset - (offset + n * 2)
  cellContent <- BG.getRemainingLazyByteString
  pure
    ( ( \pointer ->
          let bs = BL.drop (fromIntegral $ pointer - contentStartOffset) cellContent
           in BG.runGet (pageCellParser pageType) bs
      )
        <$> cellPointers
    )

type TableRecord = [TableRecordValue]

data TableRecordValue
  = NullRecord
  | Int8Record Word8
  | Int16Record Word16
  | Int24Record Word32
  | Int32Record Word32
  | Int48Record Word64
  | Int64Record Word64
  | Float64Record Double
  | ZeroRecord
  | OneRecord
  | InternalRecord
  | StringRecord Text
  | BlobRecord BL.ByteString

instance Show TableRecordValue where
  show :: TableRecordValue -> String
  show NullRecord = "NUL"
  show (StringRecord s) = T.unpack s
  show (Float64Record f) = show f
  show ZeroRecord = "0"
  show OneRecord = "1"
  show InternalRecord = ""
  show (BlobRecord _) = "<blob>"
  show r = show $ getIntegerValue r

getIntegerValue :: TableRecordValue -> Word64
getIntegerValue (Int8Record x) = fromIntegral x
getIntegerValue (Int16Record x) = fromIntegral x
getIntegerValue (Int32Record x) = fromIntegral x
getIntegerValue (Int48Record x) = fromIntegral x
getIntegerValue (Int64Record x) = fromIntegral x
getIntegerValue _ = error "Value is not integer"

recordParser :: BG.Get TableRecord
recordParser = do
  (headerSize, consumedSize) <- variantParserWithSize
  columns <- columnsParser (headerSize - fromIntegral consumedSize)
  mapM valueParser columns
 where
  valueParser :: Word64 -> BG.Get TableRecordValue
  valueParser typ = do
    case typ of
      0 -> pure NullRecord
      1 -> Int8Record <$> BG.getWord8
      2 -> Int16Record <$> BG.getWord16be
      3 -> Int24Record <$> getWord24
      4 -> Int32Record <$> BG.getWord32be
      5 -> Int48Record <$> getWord48
      6 -> Int64Record <$> BG.getWord64be
      7 -> Float64Record <$> BG.getDoublebe
      8 -> pure ZeroRecord
      9 -> pure OneRecord
      10 -> pure InternalRecord
      11 -> pure InternalRecord
      _ ->
        if even typ
          then
            BlobRecord <$> BG.getLazyByteString (fromIntegral $ (typ - 12) `div` 2)
          else do
            s <- BG.getLazyByteString (fromIntegral $ (typ - 13) `div` 2)
            -- assuming encoding is latin1
            pure $ StringRecord $ decodeLatin1 s

  getWord48 :: BG.Get Word64
  getWord48 = do
    a <- BG.getWord16be
    b <- BG.getWord32be
    pure $ (fromIntegral a `shiftL` 32) .|. fromIntegral b

  getWord24 :: BG.Get Word32
  getWord24 = do
    a <- BG.getWord8
    b <- BG.getWord16be
    pure $ (fromIntegral a `shiftL` 16) .|. fromIntegral b

  columnsParser :: Word64 -> BG.Get [Word64]
  columnsParser size =
    if size == 0
      then pure []
      else do
        (column, consumedSize) <- variantParserWithSize
        (column :) <$> columnsParser (size - fromIntegral consumedSize)

data ObjectType = TableObject | IndexObject | ViewObject | TriggerObject
  deriving (Show)

textToObjectType :: Text -> ObjectType
textToObjectType "table" = TableObject
textToObjectType "index" = IndexObject
textToObjectType "view" = ViewObject
textToObjectType "trigger" = TriggerObject
textToObjectType x = error $ "Unknown object type: " <> show x

data ObjectScema = ObjectScema
  { _objectType :: ObjectType
  , _objectName :: Text
  , _objectTblName :: Text
  , _objectRootPage :: Word64
  , _objectSql :: Text
  }
  deriving (Show)

objectSchemaParser :: BG.Get ObjectScema
objectSchemaParser = do
  [ StringRecord typ
    , StringRecord name
    , StringRecord tblName
    , rootPage
    , StringRecord sql
    ] <-
    recordParser
  pure $
    ObjectScema
      { _objectType = textToObjectType typ
      , _objectName = name
      , _objectTblName = tblName
      , _objectRootPage = getIntegerValue rootPage
      , _objectSql = sql
      }

pageParser :: Word16 -> BG.Get (PageHeader, [PageCell])
pageParser offset = do
  (header, headerSize) <- pageHeaderParser
  -- assume page header is always 8 bytes
  cells <- pageCellsParser (offset + headerSize) header
  pure (header, cells)

schemaTableParser :: Word16 -> BG.Get [ObjectScema]
schemaTableParser offset = do
  (_, cells) <- pageParser offset
  pure $
    mapMaybe
      ( \case
          TableLeafCell
            { _recordValues =
              [ StringRecord typ
                , StringRecord name
                , StringRecord tblName
                , rootPage
                , StringRecord sql
                ]
            } ->
              Just
                ObjectScema
                  { _objectType = textToObjectType typ
                  , _objectName = name
                  , _objectTblName = tblName
                  , _objectRootPage = getIntegerValue rootPage
                  , _objectSql = sql
                  }
          _ -> Nothing
      )
      cells

isTable :: ObjectScema -> Bool
isTable ObjectScema{_objectType = TableObject} = True
isTable _ = False

main :: IO ()
main = do
  (dbFilePath : cmd : _) <- getArgs
  case cmd of
    ".dbinfo" -> do
      content <- BL.readFile dbFilePath
      let (dbHeader, schemas) =
            BG.runGet
              ( do
                  dbHeader <- databaseHeaderParser
                  schemas <- schemaTableParser 100
                  pure (dbHeader, schemas)
              )
              content
      print $ "database page size: " <> show (_dbPageSize dbHeader)
      print $ "number of tables: " <> show (length $ filter isTable schemas)
      pure ()
    ".tables" -> do
      content <- BL.readFile dbFilePath
      let (dbHeader, schemas) =
            BG.runGet
              ( do
                  dbHeader <- databaseHeaderParser
                  schemas <- schemaTableParser 100
                  pure (dbHeader, schemas)
              )
              content
      let names = _objectTblName <$> filter isTable schemas
      IO.putStr $ T.unwords names
    sql -> do
      let expr = Sql.parseQueryExpr sqlDialet "" Nothing $ T.toStrict $ T.pack sql
      case expr of
        Right Sql.Select{qeSelectList = [(Sql.App [Sql.Name _ "count"] _, _)], qeFrom = [Sql.TRSimple [Sql.Name _ tableName]]} -> do
          n <- selectCountStar dbFilePath (T.fromStrict tableName)
          print n
        Right
          Sql.Select
            { qeSelectList = idens
            , qeFrom = [Sql.TRSimple [Sql.Name _ tableName]]
            , qeWhere = whereCond
            } -> do
            let columnNames = (\(Sql.Iden [Sql.Name _ columnName], _) -> T.fromStrict columnName) <$> idens
            selectColumns
              dbFilePath
              (T.fromStrict tableName)
              columnNames
              ( \values ->
                  case whereCond of
                    Just (Sql.BinOp (Sql.Iden [Sql.Name _ name]) [Sql.Name _ "="] (Sql.StringLit "'" "'" s)) ->
                      case lookup (T.fromStrict name) values of
                        Just (StringRecord valStr) ->
                          T.fromStrict s == valStr
                        _ -> False
                    Just _ -> error "unsupport"
                    Nothing -> True
              )
        Right x -> print x
        x -> do
          error "execute sql error"

sqlDialet :: Dialect
sqlDialet =
  ansi2011{diAutoincrement = True}

selectColumns :: FilePath -> Text -> [Text] -> ([(Text, TableRecordValue)] -> Bool) -> IO ()
selectColumns dbFilePath tableName columnNames pred = do
  content <- BL.readFile dbFilePath
  let (pageSize, table) =
        BG.runGet
          ( do
              dbHeader <- databaseHeaderParser
              schemas <- schemaTableParser 100
              case filter (\obj -> isTable obj && _objectTblName obj == tableName) schemas of
                [table] -> pure (_dbPageSize dbHeader, table)
                _ -> fail $ "table \"" <> T.unpack tableName <> "\" not found"
          )
          content
  pageBS <- mmapDbPage dbFilePath pageSize $ _objectRootPage table
  let (_, cells) = BG.runGet (pageParser 0) pageBS
  let schemaColumnNames = getSchemaColumnNames (_objectSql table)
  let columnIndexes = mapMaybe (`elemIndex` schemaColumnNames) columnNames
  mapM_
    ( \case
        TableLeafCell cell payload ->
          ( when (pred (zip schemaColumnNames payload)) $
              putStrLn $
                intercalate "|" $
                  fmap (\i -> show $ payload !! i) columnIndexes
          )
    )
    cells
 where
  getSchemaColumnNames :: Text -> [Text]
  getSchemaColumnNames sql =
    case Sql.parseStatement sqlDialet "" Nothing $ T.toStrict sql of
      Right (Sql.CreateTable _ columns) ->
        mapMaybe
          ( \case
              Sql.TableColumnDef (Sql.ColumnDef (Sql.Name _ name) _ _ _) ->
                Just $ T.fromStrict name
              _ -> Nothing
          )
          columns
      Left err ->
        []

selectCountStar :: FilePath -> Text -> IO Int
selectCountStar dbFilePath tableName = do
  content <- BL.readFile dbFilePath
  let (pageSize, table) =
        BG.runGet
          ( do
              dbHeader <- databaseHeaderParser
              schemas <- schemaTableParser 100
              case filter (\obj -> isTable obj && _objectTblName obj == tableName) schemas of
                [table] -> pure (_dbPageSize dbHeader, table)
                _ -> fail $ "table \"" <> T.unpack tableName <> "\" not found"
          )
          content
  pageBS <- mmapDbPage dbFilePath pageSize $ _objectRootPage table
  let (_, cells) = BG.runGet (pageParser 0) pageBS
  pure $ length cells

mmapDbPage :: FilePath -> Word16 -> Word64 -> IO BL.ByteString
mmapDbPage path pageSize pageIndex =
  mmapFileByteStringLazy
    path
    ( Just
        ( fromIntegral pageSize * fromIntegral (pageIndex - 1)
        , fromIntegral pageSize
        )
    )