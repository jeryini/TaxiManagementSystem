package com.jernejerin.traffic.helper;

import com.jernejerin.traffic.evaluation.Measurement;
import org.jxls.command.CellRefGenerator;
import org.jxls.common.CellRef;
import org.jxls.common.Context;

import java.util.ArrayList;

/**
 * Represents helper class for generating Excel with multiple sheets.
 *
 * @author Jernej Jerin
 */
public class SimpleCellRefGenerator implements CellRefGenerator {
    public CellRef generateCellRef(int index, Context context) {
        return new CellRef(
                (((ArrayList) context.getVar("measurements")).size() > index ?
                        ((Measurement)((ArrayList) context.getVar("measurements")).get(index)).name : "Result") + "!A1");
    }
}