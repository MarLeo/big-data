package com.hadoop.project;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * Created by marti on 22/01/2017.
 */
public class Driver {

    public static void main(String[] args) {

        final Logger LOGGER = LogManager.getLogger(Driver.class);
        LOGGER.info(String.format("Lauching %s at %s", Driver.class.getSimpleName(), new SimpleDateFormat("dd/MM/yyyy HH:mm:ss").format(Calendar.getInstance().getTime())));

    }
}
