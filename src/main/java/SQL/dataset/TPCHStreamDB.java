package SQL.dataset;


import java.util.Arrays;
import java.util.stream.Stream;

public abstract class TPCHStreamDB {

    public abstract Stream<CustomerRow> customer();

    public abstract Stream<LineItemRow> lineitem();

    public abstract Stream<NationRow> nation();

    public abstract Stream<OrdersRow> orders();

    public abstract Stream<PartRow> part();

    public abstract Stream<PartSuppRow> partsupp();

    public abstract Stream<RegionRow> region();

    public abstract Stream<SupplierRow> supplier();

    public abstract CustomerRow[] customer_arr();

    public abstract LineItemRow[] lineitem_arr();

    public abstract NationRow[] nation_arr();

    public abstract OrdersRow[] orders_arr();

    public abstract PartRow[] part_arr();

    public abstract PartSuppRow[] partsupp_arr();

    public abstract RegionRow[] region_arr();

    public abstract SupplierRow[] supplier_arr();

    private static Impl implementation = null;

    public static TPCHStreamDB get() {
        if (implementation == null)
            implementation = new Impl();
        return implementation;
    }

    public TPCHStreamDB parallel() {
        return new TPCHStreamDB() {
            @Override
            public Stream<CustomerRow> customer() {
                return TPCHStreamDB.this.customer().parallel();
            }

            @Override
            public Stream<LineItemRow> lineitem() {
                return TPCHStreamDB.this.lineitem().parallel();
            }

            @Override
            public Stream<NationRow> nation() {
                return TPCHStreamDB.this.nation().parallel();
            }

            @Override
            public Stream<OrdersRow> orders() {
                return TPCHStreamDB.this.orders().parallel();
            }

            @Override
            public Stream<PartRow> part() {
                return TPCHStreamDB.this.part().parallel();
            }

            @Override
            public Stream<PartSuppRow> partsupp() {
                return TPCHStreamDB.this.partsupp().parallel();
            }

            @Override
            public Stream<RegionRow> region() {
                return TPCHStreamDB.this.region().parallel();
            }

            @Override
            public Stream<SupplierRow> supplier() {
                return TPCHStreamDB.this.supplier().parallel();
            }

            @Override
            public CustomerRow[] customer_arr() {
                return TPCHStreamDB.this.customer_arr();
            }

            @Override
            public LineItemRow[] lineitem_arr() {
                return TPCHStreamDB.this.lineitem_arr();
            }

            @Override
            public NationRow[] nation_arr() {
                return TPCHStreamDB.this.nation_arr();
            }

            @Override
            public OrdersRow[] orders_arr() {
                return TPCHStreamDB.this.orders_arr();
            }

            @Override
            public PartRow[] part_arr() {
                return TPCHStreamDB.this.part_arr();
            }

            @Override
            public PartSuppRow[] partsupp_arr() {
                return TPCHStreamDB.this.partsupp_arr();
            }

            @Override
            public RegionRow[] region_arr() {
                return TPCHStreamDB.this.region_arr();
            }

            @Override
            public SupplierRow[] supplier_arr() {
                return TPCHStreamDB.this.supplier_arr();
            }
        };
    }


    static final class Impl extends TPCHStreamDB {
        static final String TPCH_PATH = System.getenv().getOrDefault("TPCH_DATA", "./data/");
        final CustomerRow[] customer;
        final LineItemRow[] lineitem;
        final NationRow[] nation;
        final OrdersRow[] orders;
        final PartRow[] part;
        final PartSuppRow[] partsupp;
        final RegionRow[] region;
        final SupplierRow[] supplier;

        private Impl() {
            this(TPCH_PATH);
        }

        private Impl(String path) {
            this.customer = new FileReader<CustomerRow>()
                    .readTBLFile(path + "customer.tbl", TableConverters.customer).toArray(CustomerRow[]::new);
            this.lineitem = new FileReader<LineItemRow>()
                    .readTBLFile(path + "lineitem.tbl", TableConverters.lineitem).toArray(LineItemRow[]::new);
            this.nation = new FileReader<NationRow>()
                    .readTBLFile(path + "nation.tbl", TableConverters.nation).toArray(NationRow[]::new);
            this.orders = new FileReader<OrdersRow>()
                    .readTBLFile(path + "orders.tbl", TableConverters.orders).toArray(OrdersRow[]::new);
            this.part = new FileReader<PartRow>()
                    .readTBLFile(path + "part.tbl", TableConverters.part).toArray(PartRow[]::new);
            this.partsupp = new FileReader<PartSuppRow>()
                    .readTBLFile(path + "partsupp.tbl", TableConverters.partsupp).toArray(PartSuppRow[]::new);
            this.region = new FileReader<RegionRow>()
                    .readTBLFile(path + "region.tbl", TableConverters.region).toArray(RegionRow[]::new);
            this.supplier = new FileReader<SupplierRow>()
                    .readTBLFile(path + "supplier.tbl", TableConverters.supplier).toArray(SupplierRow[]::new);
        }

        @Override
        public Stream<CustomerRow> customer() {
            return Arrays.stream(customer);
        }

        @Override
        public Stream<LineItemRow> lineitem() {
            return Arrays.stream(lineitem);
        }

        @Override
        public Stream<NationRow> nation() {
            return Arrays.stream(nation);
        }

        @Override
        public Stream<OrdersRow> orders() {
            return Arrays.stream(orders);
        }

        @Override
        public Stream<PartRow> part() {
            return Arrays.stream(part);
        }

        @Override
        public Stream<PartSuppRow> partsupp() {
            return Arrays.stream(partsupp);
        }

        @Override
        public Stream<RegionRow> region() {
            return Arrays.stream(region);
        }

        @Override
        public Stream<SupplierRow> supplier() {
            return Arrays.stream(supplier);
        }

        @Override
        public CustomerRow[] customer_arr() {
            return customer;
        }

        @Override
        public LineItemRow[] lineitem_arr() {
            return lineitem;
        }

        @Override
        public NationRow[] nation_arr() {
            return nation;
        }

        @Override
        public OrdersRow[] orders_arr() {
            return orders;
        }

        @Override
        public PartRow[] part_arr() {
            return part;
        }

        @Override
        public PartSuppRow[] partsupp_arr() {
            return partsupp;
        }

        @Override
        public RegionRow[] region_arr() {
            return region;
        }

        @Override
        public SupplierRow[] supplier_arr() {
            return supplier;
        }
    }

}
