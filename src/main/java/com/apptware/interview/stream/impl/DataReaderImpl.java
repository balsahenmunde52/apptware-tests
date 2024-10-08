package com.apptware.interview.stream.impl;

import com.apptware.interview.stream.DataReader;
import com.apptware.interview.stream.PaginationService;
import jakarta.annotation.Nonnull;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
class DataReaderImpl implements DataReader {

  @Autowired 
  private PaginationService paginationService;

  @Override
  public Stream<String> fetchLimitadData(int limit) {
    log.info("Fetching limited data with limit: {}", limit);
    // Fetch the data stream and limit it to the specified number of elements
    return fetchPaginatedDataAsStream().limit(limit);
  }

  @Override
  public Stream<String> fetchFullData() {
    log.info("Fetching full data without limit.");
    // Fetch all paginated data as a stream
    return fetchPaginatedDataAsStream();
  }

  /**
   * This method fetches paginated data as a stream.
   */
  private @Nonnull Stream<String> fetchPaginatedDataAsStream() {
    log.info("Fetching paginated data as stream.");
    
    int page = 1;          // Start from the first page
    int pageSize = 10;      // Define a page size (can be adjusted)
    
    return Stream.generate(() -> paginationService.getPaginatedData(page++, pageSize))
                 .takeWhile(pageData -> !pageData.isEmpty())   // Continue while there is data
                 .flatMap(List::stream)                       // Flatten the List<String> into a Stream<String>
                 .peek(item -> log.info("Fetched Item: {}", item));  // Log each item
  }
}
