package vaccination.state;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

public class CassandraWriter {
    private static final String CASSANDRA_HOST = "cassandra";
    private static final String CASSANDRA_KEYSPACE = "hadoop";
    private static final String CASSANDRA_TABLE = "state_mmr";

    private final Mapper<VaccinationData> mapper;
    public CassandraWriter() {
        Cluster cluster = Cluster.builder().addContactPoint(CASSANDRA_HOST).build();
        Session session = cluster.connect(CASSANDRA_KEYSPACE);
        MappingManager manager = new MappingManager(session);
        this.mapper = manager.mapper(VaccinationData.class);
    }

    public void writeVaccinationData(String state, double vaccinationRate) {
        VaccinationData data = new VaccinationData(state, vaccinationRate);
        this.mapper.save(data);
    }

    @Table(keyspace = CASSANDRA_KEYSPACE, name = CASSANDRA_TABLE)
    public static class VaccinationData {
        @Column(name = "state")
        private String state;
        @Column(name = "mmr")
        private double vaccinationRate;

        public VaccinationData(String state, double vaccinationRate) {
            this.state = state;
            this.vaccinationRate = vaccinationRate;
        }
    }
}
