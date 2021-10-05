package io.jdbd.postgre.util;

import io.jdbd.vendor.util.JdbdNumbers;
import reactor.util.annotation.Nullable;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.util.Locale;

public abstract class PgNumbers extends JdbdNumbers {


    @Nullable
    public static DecimalFormat getMoneyFormat(final Locale locale) {
        final NumberFormat format = NumberFormat.getCurrencyInstance(locale);
        if (!(format instanceof DecimalFormat)) {
            return null;
        }
        DecimalFormat df = (DecimalFormat) format;
        df.setParseBigDecimal(true);
        df.setRoundingMode(RoundingMode.HALF_UP);

        df.setMaximumIntegerDigits(17);
        df.setMinimumIntegerDigits(17);
        df.setMaximumFractionDigits(2);
        df.setMinimumFractionDigits(0);


        final DecimalFormatSymbols symbols = df.getDecimalFormatSymbols();
        switch (locale.toString()) {
            case "nl_BE":
            case "fr_BE":
            case "es_ES":
            case "el_GR": {
                symbols.setMonetaryDecimalSeparator(',');
                symbols.setGroupingSeparator('.');

                df.setPositivePrefix("");
                df.setPositiveSuffix(" Eu");
                df.setNegativePrefix("-");
                df.setNegativeSuffix(" Eu");
            }
            break;
            // format couldn't parse with same MonetaryDecimalSeparator and GroupingSeparator
//            case "pt_PT": {
//                symbols.setMonetaryDecimalSeparator('.');
//                symbols.setGroupingSeparator('.');
//
//                df.setPositivePrefix("");
//                df.setPositiveSuffix(" Eu");
//                df.setNegativePrefix("-");
//                df.setNegativeSuffix(" Eu");
//            }
//            break;
            case "zh_TW": {
                symbols.setMonetaryDecimalSeparator('.');
                symbols.setGroupingSeparator(',');

                df.setPositivePrefix("NT$");
                df.setPositiveSuffix("");
                df.setNegativePrefix("NT$-");
                df.setNegativeSuffix("");
            }
            break;
            case "da_DK": {
                symbols.setMonetaryDecimalSeparator(',');
                symbols.setGroupingSeparator('.');

                df.setPositivePrefix("kr ");
                df.setPositiveSuffix("");
                df.setNegativePrefix("kr -");
                df.setNegativeSuffix("");
            }
            break;
            case "en_CA":
            case "en_AU":
            case "en_US":
            case "en_NZ": {
                symbols.setMonetaryDecimalSeparator('.');
                symbols.setGroupingSeparator(',');

                df.setPositivePrefix("$");
                df.setPositiveSuffix("");
                df.setNegativePrefix("-$");
                df.setNegativeSuffix("");
            }
            break;
            case "fi_FI": {
                symbols.setMonetaryDecimalSeparator(',');
                symbols.setGroupingSeparator('.');

                df.setPositivePrefix(" ");
                df.setPositiveSuffix("Eu");
                df.setNegativePrefix("");
                df.setNegativeSuffix("Eu -");
            }
            break;
            case "fr_CH":
            case "de_CH":
            case "it_CH": {
                symbols.setMonetaryDecimalSeparator(',');
                symbols.setGroupingSeparator('.');

                df.setPositivePrefix("Fr. ");
                df.setPositiveSuffix("");
                df.setNegativePrefix("Fr.- ");
                df.setNegativeSuffix("");
            }
            break;
            case "pt_BR": {
                symbols.setMonetaryDecimalSeparator(',');
                symbols.setGroupingSeparator('.');

                df.setPositivePrefix("");
                df.setPositiveSuffix(" R$");
                df.setNegativePrefix("-");
                df.setNegativeSuffix(" R$");
            }
            break;
            case "is_IS": {
                symbols.setMonetaryDecimalSeparator('\0');
                symbols.setGroupingSeparator('.');
                df.setMaximumFractionDigits(0);

                df.setPositivePrefix("");
                df.setPositiveSuffix(" kr");
                df.setNegativePrefix("-");
                df.setNegativeSuffix(" kr");
            }
            break;
            case "et_EE": {
                symbols.setMonetaryDecimalSeparator('.');
                symbols.setGroupingSeparator(' ');

                df.setPositivePrefix("");
                df.setPositiveSuffix(" kr");
                df.setNegativePrefix("-");
                df.setNegativeSuffix(" kr");
            }
            break;
            case "ca_ES":
            case "it_IT": {
                symbols.setMonetaryDecimalSeparator(',');
                symbols.setGroupingSeparator('.');

                df.setPositivePrefix("Eu ");
                df.setPositiveSuffix("");
                df.setNegativePrefix("-Eu ");
                df.setNegativeSuffix("");
            }
            break;
            case "ko_KR": {
                symbols.setMonetaryDecimalSeparator('\0');
                symbols.setGroupingSeparator(',');
                df.setMaximumFractionDigits(0);

                df.setPositivePrefix("₩");
                df.setPositiveSuffix("");
                df.setNegativePrefix("₩-");
                df.setNegativeSuffix("");
            }
            break;
            case "zh_HK": {
                symbols.setMonetaryDecimalSeparator('.');
                symbols.setGroupingSeparator(',');

                df.setPositivePrefix("HK$");
                df.setPositiveSuffix("");
                df.setNegativePrefix("(HK$");
                df.setNegativeSuffix(")");
            }
            break;
            case "tr_TR": {
                symbols.setMonetaryDecimalSeparator(',');
                symbols.setGroupingSeparator('.');

                df.setPositivePrefix("L ");
                df.setPositiveSuffix("");
                df.setNegativePrefix("-L ");
                df.setNegativeSuffix("");
            }
            break;
            case "no_NO": {
                symbols.setMonetaryDecimalSeparator(',');
                symbols.setGroupingSeparator('.');

                df.setPositivePrefix("kr");
                df.setPositiveSuffix("");
                df.setNegativePrefix("kr-");
                df.setNegativeSuffix("");
            }
            break;
            case "de_DE": {
                symbols.setMonetaryDecimalSeparator(',');
                symbols.setGroupingSeparator('.');

                df.setPositivePrefix("Eu");
                df.setPositiveSuffix("");
                df.setNegativePrefix("-Eu");
                df.setNegativeSuffix("");
            }
            break;
            case "en_IE": {
                symbols.setMonetaryDecimalSeparator('.');
                symbols.setGroupingSeparator(',');

                df.setPositivePrefix("€");
                df.setPositiveSuffix("");
                df.setNegativePrefix("-€");
                df.setNegativeSuffix("");
            }
            break;
            case "ja_JP": {
                symbols.setMonetaryDecimalSeparator('.');
                symbols.setGroupingSeparator(',');
                df.setMaximumFractionDigits(0);

                df.setPositivePrefix("¥");
                df.setPositiveSuffix("");
                df.setNegativePrefix("¥-");
                df.setNegativeSuffix("");
            }
            break;
            case "zh_CN": {
                symbols.setMonetaryDecimalSeparator('.');
                symbols.setGroupingSeparator(',');

                df.setPositivePrefix("￥");
                df.setPositiveSuffix("");
                df.setNegativePrefix("￥-");
                df.setNegativeSuffix("");
            }
            break;
            case "en_GB": {
                symbols.setMonetaryDecimalSeparator('.');
                symbols.setGroupingSeparator(',');

                df.setPositivePrefix("£");
                df.setPositiveSuffix("");
                df.setNegativePrefix("-£");
                df.setNegativeSuffix("");
            }
            break;
            default:
                df = null;

        }
        if (df != null) {
            df.setDecimalFormatSymbols(symbols);
        }
        return df;
    }


}
