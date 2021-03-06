module MingleEvents
  class Poller
    
    # Manages a full sweep of event processing across each processing pipeline
    # configured for specified mingle projects. processors_by_project_identifier should
    # be a hash where the keys are mingle project identifiers and the values are
    # lists of event processors.
    def initialize(mingle_access, processors_by_project_identifier, state_dir=nil)
      @mingle_access = mingle_access
      @processors_by_project_identifier = processors_by_project_identifier
      @state_dir = state_dir
    end

    # Run a single poll for each project configured with processor(s) and 
    # broadcast each event to each processor.
    def run_once
      MingleEvents.log.info("MingleEvents::Poller about to poll once...")
      @processors_by_project_identifier.each do |project_identifier, processors|
        fetcher = ProjectEventFetcher.new(project_identifier, @mingle_access, @state_dir)
        fetcher.set_current_state_to_now_if_no_current_state 
        latest_events = fetcher.fetch_latest.to_a
        processors.each{|p| p.process_events(latest_events)}        
      end
    end
    
  end
end