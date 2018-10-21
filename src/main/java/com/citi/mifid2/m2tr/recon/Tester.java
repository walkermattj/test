package com.citi.mifid2.m2tr.recon;

import org.apache.commons.lang3.StringUtils;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class Tester {
    private static final OffsetDateTime START_DATE = OffsetDateTime.of(2018,1,1,0,0,0,0, ZoneOffset.UTC);
    private static final OffsetDateTime END_DATE = OffsetDateTime.of(2018,12,31,0,0,0,0, ZoneOffset.UTC);
    private static final int TIME_SPAN_HOURS = 1;
    private static final Map<String, Set<ReconTrade>> OCEAN_DATABASE = new HashMap<>();
    private static final Map<String, Set<ReconTrade>> MONGO_DATABASE = new HashMap<>();

    static {
        OffsetDateTime databaseRecordsStartDate = START_DATE;
        OffsetDateTime databaseRecordsEndDate = null;
        String recordsId = null;
        ReconTrade reconTrade = null;
        while(databaseRecordsStartDate.isBefore(END_DATE)) {
            databaseRecordsEndDate = databaseRecordsStartDate.plusHours(TIME_SPAN_HOURS);
            if(databaseRecordsEndDate.isAfter(END_DATE)) {
                databaseRecordsEndDate = END_DATE;
            }

            recordsId = String.format("%s-%s", databaseRecordsStartDate.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME), databaseRecordsEndDate.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
            OCEAN_DATABASE.put(recordsId, new HashSet<>());
            MONGO_DATABASE.put(recordsId, new HashSet<>());

            int totalTradeVersions = new Random().nextInt(3) + 1;
            for(int tradeVersionIndex = 1; tradeVersionIndex <= totalTradeVersions; tradeVersionIndex++) {
                reconTrade = new ReconTrade(Integer.parseInt(String.format("%s%s", StringUtils.leftPad(String.valueOf(databaseRecordsStartDate.getDayOfYear()), 3, '0'), StringUtils.leftPad(String.valueOf(databaseRecordsStartDate.getHour()), 2, '0'))), Integer.parseInt(String.format("%s%s%d", StringUtils.leftPad(String.valueOf(databaseRecordsStartDate.getDayOfYear()), 3, '0'), StringUtils.leftPad(String.valueOf(databaseRecordsStartDate.getHour()), 2, '0'), tradeVersionIndex)), tradeVersionIndex);
                OCEAN_DATABASE.get(recordsId).add(reconTrade);
                if((new Random().nextInt(100) + 1) < 95) {
                    MONGO_DATABASE.get(recordsId).add(reconTrade);
                }
            }
            databaseRecordsStartDate = databaseRecordsEndDate;
        }

        int totalOceanTrades = 0;
        for(String key : OCEAN_DATABASE.keySet()) {
            totalOceanTrades += OCEAN_DATABASE.get(key).size();
        }
        System.out.println(String.format("Total Ocean Trades: %d", totalOceanTrades));

        int totalMongoTrades = 0;
        for(String key : MONGO_DATABASE.keySet()) {
            totalMongoTrades += MONGO_DATABASE.get(key).size();
        }
        System.out.println(String.format("Total Mongo Trades: %d", totalMongoTrades));
        System.out.println(String.format("Difference: %d", totalOceanTrades - totalMongoTrades));
        System.out.println();
        System.out.println();
    }

    public static void replayReconTrades(OffsetDateTime startDate, OffsetDateTime endDate, int timeSpanHours) {
        if(startDate != null && endDate != null && timeSpanHours > 0) {
            List<String> reconTradeTaskIds = new ArrayList<>();
            OffsetDateTime reconTaskStartDate = startDate;
            OffsetDateTime reconTaskEndDate = null;
            while(reconTaskStartDate.isBefore(endDate)) {
                reconTaskEndDate = reconTaskStartDate.plusHours(timeSpanHours);
                if(reconTaskEndDate.isAfter(endDate)) {
                    reconTaskEndDate = endDate;
                }
                reconTradeTaskIds.add(String.format("%s-%s", reconTaskStartDate.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME), reconTaskEndDate.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)));
                reconTaskStartDate = reconTaskEndDate;
            }

            List<CompletableFuture<Set<ReconTrade>>> replayReconTradeTasks = reconTradeTaskIds.stream()
                    .map(reconTradeTaskId -> replayReconTrades(reconTradeTaskId))
                    .collect(Collectors.toList());

            CompletableFuture<Void> allReplayReconTradeTasks = CompletableFuture.allOf(
                    replayReconTradeTasks.toArray(new CompletableFuture[reconTradeTaskIds.size()]));

            CompletableFuture<List<Set<ReconTrade>>> allReplayReconTrades = allReplayReconTradeTasks.thenApply(v -> {
                return replayReconTradeTasks.stream()
                        .map(reconTrade -> reconTrade.join())
                        .collect(Collectors.toList());
            });

            CompletableFuture<Set<ReconTrade>> replayReconTrades = allReplayReconTrades.thenApply(replayReconTrade -> {
                Set<ReconTrade> allReplayTrades = new HashSet<>();
                replayReconTrade
                        .stream()
                        .forEach(ids -> allReplayTrades.addAll(ids));
                return allReplayTrades;
            });

            try {
                Set<ReconTrade> replays = new TreeSet<>(replayReconTrades.get());
                System.out.println(String.format("Total Replays: %d", replays.size()));
                for(ReconTrade reconTrade : replays) {
                    System.out.println(reconTrade.toString());
                }
                System.out.println();
                System.out.println();

                Set<ReconTrade> missingReconTrades = new HashSet<>();
                for(String key : OCEAN_DATABASE.keySet()){
                    for(ReconTrade reconTrade : OCEAN_DATABASE.get(key)) {
                        if(!MONGO_DATABASE.get(key).contains(reconTrade)) {
                            missingReconTrades.add(reconTrade);
                        }
                    }
                }
                System.out.println(String.format("Total Expected Replays: %d", missingReconTrades.size()));

                for(ReconTrade reconTrade : replays) {
                    missingReconTrades.remove(reconTrade);
                }

                System.out.println(String.format("Not Found Expected Replays: %d", missingReconTrades.size()));
                for(ReconTrade reconTrade : missingReconTrades) {
                    System.out.println(reconTrade.toString());
                }



            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    private static CompletableFuture<Set<ReconTrade>> replayReconTrades(String reconTaskId) {
        return oceanQuery(reconTaskId).thenCombineAsync(mongoQuery(reconTaskId), (oceanSet, mongoSet) -> {
            Set<ReconTrade> replayReconTrades = new HashSet();
            if (oceanSet != null && !oceanSet.isEmpty()) {
                for (ReconTrade reconTrade : oceanSet) {
                    if (mongoSet == null || mongoSet.isEmpty() || !mongoSet.contains(reconTrade)) {
                        replayReconTrades.add(reconTrade);
                    }
                }
            }
            return replayReconTrades;
        });
    }

    private static CompletableFuture<Set<ReconTrade>> oceanQuery(String reconTaskId) {
        return CompletableFuture.supplyAsync(() -> {
            return OCEAN_DATABASE.get(reconTaskId);
        });
    }

    private static CompletableFuture<Set<ReconTrade>> mongoQuery(String reconTaskId) {
        return CompletableFuture.supplyAsync(() -> {
            return MONGO_DATABASE.get(reconTaskId);
        });
    }

    public static void main(String [] args) {
        replayReconTrades(START_DATE, END_DATE, TIME_SPAN_HOURS);
    }
}