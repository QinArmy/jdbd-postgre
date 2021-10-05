package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.stmt.PgStmts;
import io.jdbd.postgre.util.PgStrings;
import io.jdbd.result.MultiResult;
import io.jdbd.result.ResultRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;
import reactor.core.publisher.Mono;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class SessionConfigUnitTests extends AbstractTaskTests {

    private static final Logger LOG = LoggerFactory.getLogger(SessionConfigUnitTests.class);


    /**
     * <p>
     * print postgre money format.
     * </p>
     *
     * @see io.jdbd.postgre.util.PgNumbers#getMoneyFormat(Locale)
     */
    @Test
    public void printMoneyFormat() {
        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);

        final Locale[] locales = Locale.getAvailableLocales();

        final List<String> correctList = new ArrayList<>(), errorList = new ArrayList<>();

        for (Locale locale : locales) {
            if (!PgStrings.hasText(locale.getCountry())) {
                continue;
            }
            List<String> sqlGroup = new ArrayList<>();
            sqlGroup.add(String.format("SET lc_monetary = '%s.UTF-8'", locale));
            sqlGroup.add("SELECT '+92233720368547758.07'::decimal::money as p,'0.00'::money as z ,'-92233720368547758.08'::decimal::money as n ");


            MultiResult result = SimpleQueryTask.batchAsMulti(PgStmts.group(sqlGroup), adjutant);
            try {
                ResultRow row = Mono.from(result.nextUpdate())
                        .thenMany(result.nextQuery())
                        .last()
                        .block();
                Assert.assertNotNull(row);
                String m;
                DecimalFormat format = (DecimalFormat) NumberFormat.getCurrencyInstance(locale);
                m = String.format("locale:%s , positive:%s ,zero:%s , negative:%s , locale symbol:%s"
                        , locale
                        , row.get(0, String.class)
                        , row.get(1, String.class)
                        , row.get(2, String.class)
                        , format.getDecimalFormatSymbols().getCurrencySymbol());
                correctList.add(m);
            } catch (Throwable e) {
                // LOG.error("",e);
                errorList.add(String.format("locale: %s ", locale));
            }

        }

        for (String value : correctList) {
            System.out.println(value);
        }
        System.out.println();
        System.out.println("----");
        System.out.println();
        for (String value : errorList) {
            System.out.println(value);
        }
        releaseConnection(protocol);

    }


}
