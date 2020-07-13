package com.ericguerra.dag.schedule;

import com.ericguerra.dag.model.DagDefinition;
import com.ericguerra.dag.service.DagService;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.Set;
import java.util.TimeZone;

/**
 * Keeps one Quartz cron trigger per DAG in sync with the YAML. Quartz runs in clustered JDBC mode,
 * so with several DagRunner nodes each fire happens on exactly one of them.
 */
@Component
@Profile("!test")
public class DagScheduleRegistrar {
    private static final Logger log = LoggerFactory.getLogger(DagScheduleRegistrar.class);
    static final String GROUP = "dags";

    private final Scheduler scheduler;
    private final DagService dags;

    public DagScheduleRegistrar(Scheduler scheduler, DagService dags) {
        this.scheduler = scheduler;
        this.dags = dags;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void sync() throws SchedulerException {
        Set<JobKey> wanted = new HashSet<>();
        for (DagDefinition d : dags.dags()) {
            if (d.getSchedule() == null) continue;
            JobKey key = JobKey.jobKey(d.getId(), GROUP);
            wanted.add(key);
            JobDetail job = JobBuilder.newJob(TriggerDagJob.class).withIdentity(key).usingJobData("dagId", d.getId()).storeDurably().build();
            Trigger trigger = TriggerBuilder.newTrigger().withIdentity(d.getId(), GROUP).forJob(job)
                    .withSchedule(CronScheduleBuilder.cronSchedule(d.getSchedule())
                            .inTimeZone(TimeZone.getTimeZone("UTC"))
                            .withMisfireHandlingInstructionFireAndProceed())
                    .build();
            scheduler.addJob(job, true);
            if (scheduler.checkExists(trigger.getKey())) scheduler.rescheduleJob(trigger.getKey(), trigger);
            else scheduler.scheduleJob(trigger);
            log.info("scheduled {} with '{}'", d.getId(), d.getSchedule());
        }
        for (JobKey existing : scheduler.getJobKeys(org.quartz.impl.matchers.GroupMatcher.jobGroupEquals(GROUP))) {
            if (!wanted.contains(existing)) {
                scheduler.deleteJob(existing);
                log.info("unscheduled removed dag {}", existing.getName());
            }
        }
    }
}
