
"""
Project: Oslovovac
File: main.py
Description: Entry point for the application that sets up configuration and executes the main processing pipeline.
Author: Jan Alexandr Kopřiva jan.alexandr.kopriva@gmail.com
License: MIT
"""
import asyncio
import logging
import logging.config
import time
import signal
import gc
import pandas as pd
from pathlib import Path

from src.config import LOGGING_CONFIG, HTTP_CONFIG, FILE_CONFIG, USER_AGENTS
from src.adapters import AdaptiveDelay, AdaptiveWorkers, AdaptiveBatchSize, UserAgentManager
from src.services import CheckpointService, NameService, BatchService
from src.utils import GracefulShutdownHandler, create_aiohttp_session

# --- MAIN APPLICATION LOGIC ---
logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger(__name__) # Logger for main module

async def process_all_names_main_task(shutdown_handler: GracefulShutdownHandler):
    start_time_total = time.time()
    logger.info("Application starting name processing...")
    
    input_file = FILE_CONFIG['INPUT_FILE']
    output_file = FILE_CONFIG['OUTPUT_FILE']

    if not Path(input_file).exists():
        logger.error(f"Input file {input_file} not found. Exiting.")
        # Create dummy file for testing if it doesn't exist
        logger.info(f"Creating dummy input file {input_file} for testing.")
        dummy_data = {'ID': range(1, 21), 'Trade_Name': [f'Jméno Příjmení {i}' for i in range(1, 21)]}
        pd.DataFrame(dummy_data).to_csv(input_file, index=False, encoding='utf-8')
        # return # Uncomment to stop if dummy data isn't desired

    delay_adapter = AdaptiveDelay(
        initial_delay=HTTP_CONFIG['INITIAL_DELAY'],
        max_delay=HTTP_CONFIG['MAX_DELAY'],
        min_delay=HTTP_CONFIG['MIN_DELAY']
    )
    worker_adapter = AdaptiveWorkers(
        initial_workers=HTTP_CONFIG.get('INITIAL_WORKERS', HTTP_CONFIG['MIN_WORKERS']),
        max_workers=HTTP_CONFIG['MAX_WORKERS'],
        min_workers=HTTP_CONFIG['MIN_WORKERS']
    )
    user_agent_manager = UserAgentManager(USER_AGENTS)
    batch_size_adapter = AdaptiveBatchSize(
        initial_size=HTTP_CONFIG['BATCH_SIZE'],
        max_size=HTTP_CONFIG.get('MAX_BATCH_SIZE', HTTP_CONFIG['BATCH_SIZE'] * 2),
        min_size=HTTP_CONFIG.get('MIN_BATCH_SIZE', max(10, HTTP_CONFIG['BATCH_SIZE'] // 4))
    )
    checkpoint_service = CheckpointService()
    
    # Optionally clear checkpoint for new run
    # checkpoint_service.clear_checkpoint_for_new_run()

    # Save checkpoint before run start to flush data
    if checkpoint_service.processed_names:
        logger.info(f"Flushing checkpoint before start: {len(checkpoint_service.processed_names)} processed names.")
        checkpoint_service.save_checkpoint(
            checkpoint_service.last_chunk_fully_processed_index,
            checkpoint_service.last_batch_completed_for_current_chunk,
            {},  # Empty dict as data is already in processed_names
            is_chunk_complete=(checkpoint_service.last_batch_completed_for_current_chunk == 0)
        )
        logger.info("Checkpoint flushed successfully before start.")

    async with create_aiohttp_session() as session:
        name_service = NameService(session, user_agent_manager)
        batch_service = BatchService(
            name_service, checkpoint_service,
            delay_adapter, worker_adapter, batch_size_adapter,
            shutdown_handler.shutdown_event
        )

        chunk_size = HTTP_CONFIG['CHUNK_SIZE']
        total_processed_rows = 0
        current_chunk_index = -1 # Start with -1, first chunk will be 0

        # Determine total rows (can be slow for very large files)
        try:
            with open(input_file, 'r', encoding='utf-8') as f_count:
                total_rows_in_file = sum(1 for _ in f_count) -1 # -1 for header
            logger.info(f"Total records to process in {input_file}: {total_rows_in_file:,}")
        except Exception as e:
            logger.warning(f"Could not determine total rows in input file: {e}")
            total_rows_in_file = -1 # Unknown count

        # Create/overwrite output file with header if not resuming or first chunk
        # Or if checkpoint says we are at the beginning
        if checkpoint_service.last_chunk_fully_processed_index == -1 and \
           checkpoint_service.last_batch_completed_for_current_chunk == 0:
            logger.info(f"Creating new output file {output_file} with headers.")
            pd.DataFrame(columns=['ID', 'Trade_Name', 'Oslovení', 'Oslovení jméno', 'Oslovení příjmení', 'Chyba']).to_csv(
                output_file, index=False, encoding='utf-8'
            )
        else:
            logger.info(f"Appending to existing output file {output_file} or resuming.")


        try:
            for chunk_df in pd.read_csv(input_file, chunksize=chunk_size, encoding='utf-8'):
                current_chunk_index += 1
                if shutdown_handler.shutdown_requested:
                    logger.info("Shutdown requested, stopping further chunk processing.")
                    break
                
                logger.info(f"\n--- Processing Chunk {current_chunk_index} ({len(chunk_df):,} records) ---")

                if checkpoint_service.last_chunk_fully_processed_index >= current_chunk_index:
                    logger.info(f"Chunk {current_chunk_index} was already fully processed. Skipping.")
                    total_processed_rows += len(chunk_df) # Add to total count
                    continue

                if 'Trade_Name' not in chunk_df.columns:
                    logger.error(f"Chunk {current_chunk_index} does not contain 'Trade_Name' column. Skipping.")
                    continue
                if 'ID' not in chunk_df.columns:
                    logger.warning(f"Chunk {current_chunk_index} does not contain 'ID' column. Generating sequential IDs for this chunk.")
                    # Generate ID relative to start of this chunk
                    chunk_df['ID'] = range(total_processed_rows + 1, total_processed_rows + len(chunk_df) + 1)

                # Clean chunk data
                original_count_in_chunk = len(chunk_df)
                chunk_df.dropna(subset=['Trade_Name'], inplace=True)
                chunk_df['Trade_Name'] = chunk_df['Trade_Name'].astype(str).str.strip() # Ensure string and strip whitespace
                chunk_df = chunk_df[chunk_df['Trade_Name'] != ''] # Remove empty Trade_Name
                # Handle duplicates carefully if ID is important. Here duplicates in Trade_Name are kept
                # as they might have different IDs. 
                # chunk_df.drop_duplicates(subset=['Trade_Name'], keep='first', inplace=True)
                
                cleaned_count_in_chunk = len(chunk_df)
                if original_count_in_chunk != cleaned_count_in_chunk:
                    logger.info(f"Cleaned chunk {current_chunk_index}: removed {original_count_in_chunk - cleaned_count_in_chunk} empty/NaN 'Trade_Name's.")

                if cleaned_count_in_chunk == 0:
                    logger.info(f"Chunk {current_chunk_index} is empty after cleaning. Skipping.")
                    continue
                
                # Initialize output columns
                for col in ['Oslovení', 'Oslovení jméno', 'Oslovení příjmení', 'Chyba']:
                    if col not in chunk_df.columns: chunk_df[col] = pd.NA

                await batch_service.process_chunk_data(chunk_df, current_chunk_index)
                
                # Append processed chunk to output file
                # Save only if we actually processed this chunk (didn't skip)
                # And if shutdown didn't happen in the middle (should be handled in batch_service)
                if not (checkpoint_service.last_chunk_fully_processed_index >= current_chunk_index and not shutdown_handler.shutdown_requested):
                    logger.info(f"Appending processed chunk {current_chunk_index} to {output_file}...")
                    chunk_df.to_csv(output_file, mode='a', header=False, index=False, encoding='utf-8')
                    logger.info(f"Chunk {current_chunk_index} successfully appended.")

                total_processed_rows += len(chunk_df)
                
                # Chunk statistics
                success_condition = (chunk_df['Oslovení'].notna()) & \
                                    (chunk_df['Oslovení'] != '') & \
                                    (chunk_df['Oslovení'].str.lower() != chunk_df['Trade_Name'].str.lower())
                actual_success_count = success_condition.sum()
                logger.info(f"--- Chunk {current_chunk_index} Finished ---")
                logger.info(f"- Processed in chunk: {len(chunk_df):,} records")
                logger.info(f"- Successfully transformed: {actual_success_count:,} names ({actual_success_count/len(chunk_df)*100:.1f}%)")
                if total_rows_in_file > 0:
                    logger.info(f"- Overall progress: {total_processed_rows:,}/{total_rows_in_file:,} records "
                                f"({(total_processed_rows/total_rows_in_file*100):.1f}%)")
                else:
                    logger.info(f"- Overall processed: {total_processed_rows:,} records")

                del chunk_df
                gc.collect()
                logger.debug("Garbage collection after chunk processing.")

        except asyncio.CancelledError:
            logger.warning("Main processing task was cancelled. Saving checkpoint if possible...")
            # Checkpoint should be saved by batch_service on shutdown event
            raise # Re-raise to be handled in main_wrapper

        except Exception as e:
            logger.critical(f"Critical error during chunk processing: {e}", exc_info=True)
            raise # Propagate error

    total_time = time.time() - start_time_total
    logger.info(f"=== Total Processing Finished ===")
    logger.info(f"Total execution time: {total_time:.2f} seconds")
    logger.info(f"Total records processed across all chunks: {total_processed_rows:,}")


async def main_wrapper():
    """Main wrapper for execution and shutdown management."""
    shutdown_handler = GracefulShutdownHandler()
    
    # Set signal handlers for asyncio loop
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown_handler.handle_signal, sig, None)

    main_task = None
    try:
        logger.info("Starting main_wrapper...")
        main_task = asyncio.create_task(process_all_names_main_task(shutdown_handler))
        
        # Wait for main task completion OR shutdown signal
        done, pending = await asyncio.wait(
            [main_task, shutdown_handler.shutdown_event.wait()],
            return_when=asyncio.FIRST_COMPLETED
        )

        if shutdown_handler.shutdown_event.is_set(): # If shutdown event triggered first
            logger.info("Shutdown event triggered. Cancelling main processing task if not done.")
            if not main_task.done():
                main_task.cancel()
                try:
                    await main_task # Wait for cancellation completion
                except asyncio.CancelledError:
                    logger.info("Main processing task successfully cancelled after shutdown signal.")
                # Other exceptions would appear here
        
        # Check main_task result if it finished on its own
        if main_task in done:
            try:
                main_task.result() # Get result or raise exception
                logger.info("Main processing task completed.")
            except asyncio.CancelledError:
                logger.info("Main processing task was cancelled (as expected or externally).")
            except Exception as e:
                logger.error(f"Main processing task failed with an exception: {e}", exc_info=True)
        
    except asyncio.CancelledError: # If main_wrapper itself is cancelled
        logger.info("Main_wrapper task itself was cancelled.")
        if main_task and not main_task.done():
            main_task.cancel()
            await asyncio.gather(main_task, return_exceptions=True) # Cleanup
            
    except Exception as e:
        logger.critical(f"Unhandled critical error in main_wrapper: {e}", exc_info=True)
    
    finally:
        logger.info("Main_wrapper finishing. Performing final cleanup of tasks...")
        # Cancel all other running tasks
        tasks_to_cancel = [task for task in asyncio.all_tasks(loop=loop) if task is not asyncio.current_task() and not task.done()]
        if tasks_to_cancel:
            logger.info(f"Cancelling {len(tasks_to_cancel)} outstanding tasks...")
            for task in tasks_to_cancel:
                task.cancel()
            await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
            logger.info("Outstanding tasks processed.")
        else:
            logger.info("No outstanding tasks to cancel.")
        logger.info("Application shutdown sequence complete.")


if __name__ == "__main__":
    try:
        asyncio.run(main_wrapper())
    except KeyboardInterrupt: # Fallback for keyboard interrupt
        logger.warning("KeyboardInterrupt caught directly in __main__. Application will exit.")
    except Exception as e:
        logger.critical(f"Critical error at top level: {e}", exc_info=True)
    finally:
        logging.shutdown()
        print("Application has exited.")