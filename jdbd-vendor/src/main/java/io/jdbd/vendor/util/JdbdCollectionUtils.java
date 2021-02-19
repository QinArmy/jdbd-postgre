package io.jdbd.vendor.util;

import io.jdbd.BindParameterException;
import io.jdbd.SQLBindParameterException;
import io.jdbd.vendor.IBindValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public abstract class JdbdCollectionUtils extends org.qinarmy.util.CollectionUtils {


    public static <T> List<List<T>> unmodifiableGroupList(final List<List<T>> groupList) {
        final List<List<T>> newGroupList;
        switch (groupList.size()) {
            case 0:
                newGroupList = Collections.emptyList();
                break;
            case 1:
                newGroupList = Collections.singletonList(groupList.get(0));
                break;
            default: {
                List<List<T>> list = new ArrayList<>(groupList.size());
                for (List<T> group : groupList) {
                    list.add(unmodifiableList(group));
                }
                newGroupList = Collections.unmodifiableList(list);
            }
        }
        return newGroupList;
    }

    /**
     * @param parameterGroup a modifiable list.
     * @return a unmodifiable list
     * @throws BindParameterException throw when parameterGroup error.
     */
    public static <T extends IBindValue> List<T> prepareParameterGroup(final List<T> parameterGroup)
            throws BindParameterException {

        parameterGroup.sort(Comparator.comparingInt(IBindValue::getParamIndex));
        final int size = parameterGroup.size();
        for (int i = 0, index; i < size; i++) {
            index = parameterGroup.get(i).getParamIndex();
            if (index == i - 1) {
                throw new BindParameterException(index, "Bind parameter[%s] duplication.", index);
            } else if (index != i) {
                throw new BindParameterException(i, "Bind parameter[%s] not set.", i);
            }
        }
        return unmodifiableList(parameterGroup);
    }


    /**
     * @param groupList a modifiable list.
     * @return a unmodifiable list
     * @throws SQLBindParameterException throw when groupList error.
     */
    public static <T extends IBindValue> List<List<T>> prepareBatchParameterGroup(List<List<T>> groupList)
            throws SQLBindParameterException {

        int parameterSize = -1;
        final int size = groupList.size();
        List<List<T>> newGroupList = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            List<T> group = groupList.get(i);
            if (parameterSize < 0) {
                parameterSize = group.size();
            } else if (group.size() != parameterSize) {
                throw new SQLBindParameterException(
                        "Bind batch[%s] parameter count[%s] and previous batch[%s] not match. "
                        , i, group.size(), i - 1);
            } else {
                newGroupList.add(prepareParameterGroup(group));
            }
        }

        return unmodifiableList(newGroupList);

    }


}
