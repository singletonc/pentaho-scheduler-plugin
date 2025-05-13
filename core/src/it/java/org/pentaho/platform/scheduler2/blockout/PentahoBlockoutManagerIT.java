/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.platform.scheduler2.blockout;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.platform.api.action.IAction;
import org.pentaho.platform.api.engine.IPluginManager;
import org.pentaho.platform.api.engine.IUserRoleListService;
import org.pentaho.platform.api.scheduler2.ComplexJobTrigger;
import org.pentaho.platform.api.scheduler2.IBlockoutManager;
import org.pentaho.platform.api.scheduler2.IJob;
import org.pentaho.platform.api.scheduler2.IJobTrigger;
import org.pentaho.platform.api.scheduler2.IScheduler;
import org.pentaho.platform.api.scheduler2.Job;
import org.pentaho.platform.api.scheduler2.SimpleJobTrigger;
import org.pentaho.platform.engine.core.system.PentahoSystem;
import org.pentaho.platform.scheduler2.blockout.BlockoutManagerUtil.TIME;
import org.pentaho.platform.scheduler2.quartz.test.StubUserDetailsService;
import org.pentaho.platform.scheduler2.quartz.test.StubUserRoleListService;
import org.pentaho.platform.scheduler2.ws.TestQuartzScheduler;
import org.pentaho.platform.scheduler2.ws.test.JaxWsSchedulerServiceIT.TstPluginManager;
import org.pentaho.test.platform.engine.core.MicroPlatform;
import org.springframework.security.core.userdetails.UserDetailsService;

/**
 * @author wseyler
 * 
 */
public class PentahoBlockoutManagerIT {

  private final long duration;

  IBlockoutManager blockOutManager;

  IScheduler scheduler;

  Set<String> jobIdsToClear = new HashSet<String>();

  public PentahoBlockoutManagerIT() {
    duration = TIME.HOUR.time * 2;
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    MicroPlatform mp = new MicroPlatform();
    mp.define( IPluginManager.class, TstPluginManager.class );
    mp.define( "IScheduler2", TestQuartzScheduler.class ); //$NON-NLS-1$
    mp.define( IUserRoleListService.class, StubUserRoleListService.class );
    mp.define( UserDetailsService.class, StubUserDetailsService.class );
    mp.define( IBlockoutManager.class, PentahoBlockoutManager.class );
    mp.start();

    blockOutManager = new PentahoBlockoutManager();
    scheduler = PentahoSystem.get( IScheduler.class, "IScheduler2", null ); //$NON-NLS-1$;

    jobIdsToClear.clear();
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {

    Set<String> jobIdsIter = new HashSet<String>( this.jobIdsToClear );
    // Clear all jobs after each test
    for ( String jobId : jobIdsIter ) {
      deleteJob( jobId );
    }
  }

  @Test
  public void testGetBlockouts() throws Exception {
    IJobTrigger trigger1 = new SimpleJobTrigger( new Date(), null, -1, 1000000 );
    IJobTrigger trigger2 = new SimpleJobTrigger( new Date(), null, -1, 1000000 );
    addBlockOutJob( trigger1 );
    addBlockOutJob( trigger2 );

    assertEquals( 2, this.blockOutManager.getBlockOutJobs().size() );
  }

  /**
   * Test method for
   * {@link org.pentaho.platform.scheduler2.blockout.PentahoBlockoutManager#willFire(org.pentaho.platform.api.scheduler2.IJobTrigger)}.
   */
  @Test
  public void testWillFire() throws Exception {
    Calendar blockOutStartDate = new GregorianCalendar( 2013, Calendar.JANUARY, 7 );
    IJobTrigger blockOutJobTrigger =
        new SimpleJobTrigger( blockOutStartDate.getTime(), null, -1, TIME.WEEK.time / 1000 );
    blockOutJobTrigger.setDuration( duration );

    /*
     * Simple Schedule Triggers
     */
    Calendar scheduleStartDate = new GregorianCalendar( 2013, Calendar.JANUARY, 7, 1, 0, 0 );
    IJobTrigger trueScheduleTrigger =
        new SimpleJobTrigger( scheduleStartDate.getTime(), null, -1, TIME.DAY.time / 1000 );

    IJobTrigger falseScheduleTrigger =
        new SimpleJobTrigger( scheduleStartDate.getTime(), null, -1, TIME.WEEK.time / 1000 );

    IJob blockOutJob = addBlockOutJob( blockOutJobTrigger );
    assertTrue( this.blockOutManager.willFire( trueScheduleTrigger ) );
    assertFalse( this.blockOutManager.willFire( falseScheduleTrigger ) );

    /*
     * Complex Schedule Triggers
     */
    IJobTrigger trueComplexScheduleTrigger = new ComplexJobTrigger();
    trueComplexScheduleTrigger.setStartTime( scheduleStartDate.getTime() );
    trueComplexScheduleTrigger.setCronString( "0 0 1 ? * 2-3 *" ); //$NON-NLS-1$

    IJobTrigger falseComplexScheduleTrigger = new ComplexJobTrigger();
    falseComplexScheduleTrigger.setStartTime( scheduleStartDate.getTime() );
    falseComplexScheduleTrigger.setCronString( "0 0 1 ? * 2 *" ); //$NON-NLS-1$

    assertTrue( this.blockOutManager.willFire( trueComplexScheduleTrigger ) );
    assertFalse( this.blockOutManager.willFire( falseComplexScheduleTrigger ) );

    /*
     * Complex Block out
     */
    deleteJob( blockOutJob.getJobId() );
    blockOutJobTrigger = new ComplexJobTrigger();
    blockOutJobTrigger.setStartTime( blockOutStartDate.getTime() );
    blockOutJobTrigger.setDuration( duration );
    blockOutJobTrigger.setCronString( "0 0 0 ? * 2 *" ); //$NON-NLS-1$

    addBlockOutJob( blockOutJobTrigger );

    assertTrue( this.blockOutManager.willFire( trueScheduleTrigger ) );
    assertFalse( this.blockOutManager.willFire( falseScheduleTrigger ) );
    assertTrue( this.blockOutManager.willFire( trueComplexScheduleTrigger ) );
    assertFalse( this.blockOutManager.willFire( falseComplexScheduleTrigger ) );

  }

  /**
   * Test method for
   * {@link org.pentaho.platform.scheduler2.blockout.PentahoBlockoutManager#isPartiallyBlocked(org.pentaho.platform.api.scheduler2.IJobTrigger)}.
   */
  @Test
  public void testIsPartiallyBlocked() throws Exception {
    Calendar blockOutStartDate = new GregorianCalendar( 2013, Calendar.JANUARY, 1, 0, 0, 0 );
    IJobTrigger blockOutTrigger =
        new SimpleJobTrigger( blockOutStartDate.getTime(), null, -1, TIME.WEEK.time * 2 / 1000 );
    blockOutTrigger.setDuration( duration );

    /*
     * Simple Schedule Triggers
     */
    Calendar trueScheduleStartDate1 = new GregorianCalendar( 2013, Calendar.JANUARY, 15, 0, 0, 0 );
    IJobTrigger trueSchedule1 =
        new SimpleJobTrigger( trueScheduleStartDate1.getTime(), null, -1, TIME.WEEK.time * 2 / 1000 );

    Calendar trueScheduleStartDate2 = new GregorianCalendar( 2013, Calendar.JANUARY, 15, 0, 0, 0 );
    IJobTrigger trueSchedule2 =
        new SimpleJobTrigger( trueScheduleStartDate2.getTime(), null, -1, TIME.WEEK.time / 1000 );

    Calendar falseScheduleStartDate1 = new GregorianCalendar( 2013, Calendar.JANUARY, 1, 3, 0, 0 );
    IJobTrigger falseSchedule1 =
        new SimpleJobTrigger( falseScheduleStartDate1.getTime(), null, -1, TIME.WEEK.time / 1000 );

    IJob blockOutJob = addBlockOutJob( blockOutTrigger );

    assertTrue( this.blockOutManager.isPartiallyBlocked( trueSchedule1 ) );
    assertTrue( this.blockOutManager.isPartiallyBlocked( trueSchedule2 ) );
    assertFalse( this.blockOutManager.isPartiallyBlocked( falseSchedule1 ) );

    /*
     * Complex Schedule Triggers
     */
    IJobTrigger trueComplexScheduleTrigger = new ComplexJobTrigger();
    trueComplexScheduleTrigger.setStartTime( trueScheduleStartDate1.getTime() );
    trueComplexScheduleTrigger.setCronString( "0 0 1 ? * 2-3 *" ); //$NON-NLS-1$

    IJobTrigger falseComplexScheduleTrigger = new ComplexJobTrigger();
    falseComplexScheduleTrigger.setStartTime( trueScheduleStartDate1.getTime() );
    falseComplexScheduleTrigger.setCronString( "0 0 1 ? * 2 *" ); //$NON-NLS-1$

    assertTrue( this.blockOutManager.isPartiallyBlocked( trueComplexScheduleTrigger ) );
    assertFalse( this.blockOutManager.isPartiallyBlocked( falseComplexScheduleTrigger ) );

    /*
     * Complex Block Out IJobTrigger
     */
    deleteJob( blockOutJob.getJobId() );
    blockOutTrigger = new ComplexJobTrigger();
    blockOutTrigger.setStartTime( blockOutStartDate.getTime() );
    blockOutTrigger.setCronString( "0 0 0 ? * 3 *" ); //$NON-NLS-1$
    blockOutTrigger.setDuration( duration );
    addBlockOutJob( blockOutTrigger );

    assertTrue( this.blockOutManager.isPartiallyBlocked( trueSchedule1 ) );
    assertTrue( this.blockOutManager.isPartiallyBlocked( trueSchedule2 ) );
    assertFalse( this.blockOutManager.isPartiallyBlocked( falseSchedule1 ) );
    assertTrue( this.blockOutManager.isPartiallyBlocked( trueComplexScheduleTrigger ) );
    assertFalse( this.blockOutManager.isPartiallyBlocked( falseComplexScheduleTrigger ) );
  }

  /**
   * Test method for {@link org.pentaho.platform.scheduler2.blockout.PentahoBlockoutManager#shouldFireNow()}.
   */
  @Test
  public void testShouldFireNow() throws Exception {
    Date blockOutStartDate = new Date( System.currentTimeMillis() );
    IJobTrigger blockOutJobTrigger = new SimpleJobTrigger( blockOutStartDate, null, -1, TIME.WEEK.time * 2 / 1000 );
    blockOutJobTrigger.setDuration( duration );

    IJob blockOutJob = addBlockOutJob( blockOutJobTrigger );

    assertFalse( this.blockOutManager.shouldFireNow() );

    deleteJob( blockOutJob.getJobId() );
    blockOutStartDate = new Date( System.currentTimeMillis() + TIME.HOUR.time );
    blockOutJobTrigger = new SimpleJobTrigger( blockOutStartDate, null, -1, TIME.WEEK.time * 2 / 1000 );
    blockOutJobTrigger.setDuration( duration );
    addBlockOutJob( blockOutJobTrigger );

    assertTrue( this.blockOutManager.shouldFireNow() );
  }

  private IJob addBlockOutJob( IJobTrigger blockOutJobTrigger ) throws Exception {
    Map<String, Object> jobParams = new HashMap<String, Object>();
    jobParams.put( IBlockoutManager.DURATION_PARAM, blockOutJobTrigger.getDuration() );

    return addJob( blockOutJobTrigger, IBlockoutManager.BLOCK_OUT_JOB_NAME, new BlockoutAction(), jobParams );
  }

  private IJob addJob( IJobTrigger jobTrigger, String jobName ) throws Exception {
    return addJob( jobTrigger, jobName, new IAction() {
      @Override
      public void execute() throws Exception {
      }
    }, new HashMap<String, Object>() );
  }

  private IJob addJob( IJobTrigger jobTrigger, String jobName, IAction action, Map<String, Object> jobParams )
    throws Exception {
    IJob job = this.scheduler.createJob( jobName, action.getClass(), jobParams, jobTrigger );
    this.jobIdsToClear.add( job.getJobId() );
    return job;
  }

  private void deleteJob( String jobId ) throws Exception {
    this.scheduler.removeJob( jobId );
    this.jobIdsToClear.remove( jobId );
  }
}
