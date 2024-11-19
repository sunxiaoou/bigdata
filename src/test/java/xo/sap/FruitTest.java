package xo.sap;

import org.junit.Test;

public class FruitTest {
    @Test
    public void scan() {
        String[] args = {"scan"};
        FruitHana.main(args);
    }

    @Test
    public void count() {
        String[] args = {"count"};
        FruitHana.main(args);
    }

    @Test
    public void truncate() {
        String[] args = {"truncate"};
        FruitHana.main(args);
    }

    @Test
    public void put() {
        String[] args = {"put"};
        FruitHana.main(args);
    }

    @Test
    public void add() {
        String[] args = {"add"};
        FruitHana.main(args);
    }

    @Test
    public void delete() {
        String[] args = {"delete"};
        FruitHana.main(args);
    }

    @Test
    public void update() {
        String[] args = {"update"};
        FruitHana.main(args);
    }

    @Test
    public void createTable() {
        String[] args = {"createTable"};
        FruitHana.main(args);
    }

    @Test
    public void dropTable() {
        String[] args = {"dropTable"};
        FruitHana.main(args);
    }
}
