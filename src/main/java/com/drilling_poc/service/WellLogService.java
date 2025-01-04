package com.drilling_poc.service;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.util.List;
import java.util.Map;
import org.springframework.jdbc.core.JdbcTemplate;

@Service
public class WellLogService {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    public List<Map<String, Object>> getWellLogs(String wellId) {
        // SQL with wellId directly injected into the query
        String sql = "SELECT well_id, sppa, cppa, rop, time, batch_no, prediction " +
                "FROM Well_Data " +
                "WHERE well_id = '" + wellId + "' " + // Directly include the wellId in the SQL query
                "ORDER BY time DESC " +
                "LIMIT 10";

        // Log SQL and parameter
        System.out.println("Executing SQL: " + sql);

        // Execute the query directly with the wellId
        return jdbcTemplate.queryForList(sql);
    }
}
